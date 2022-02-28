import os

import functools
import tarfile

# from tempfile import NamedTemporaryFile
from typing import Optional  # , Union, BinaryIO
from dbt.clients import system
from dbt.deps.base import PinnedPackage, UnpinnedPackage, get_downloads_path
from dbt.exceptions import (
    DependencyException,
    package_structure_malformed,
    package_sha1_fail,
)
from dbt.events.functions import fire_event
from dbt.events.types import (
    Sha1ChecksumPasses,
    UntarProjectRoot,
    TarballReceivedFeedback,
    CopyFeedback,
)
from dbt.config import Project
from dbt.contracts.project import (
    ProjectPackageMetadata,
    TarballPackage,
)
from dbt.utils import _connection_exception_retry as connection_exception_retry


TARFILE_MAX_SIZE = 1 * 1e6  # limit tarfiles to 1mb


class TarballPackageMixin:
    def __init__(self, tarball: str) -> None:
        super().__init__()
        self.tarball = tarball

    @property
    def name(self):
        return self.tarball

    def source_type(self) -> str:
        return "tarball"


class TarballPinnedPackage(TarballPackageMixin, PinnedPackage):
    def __init__(
        self,
        tarball: Union[str, tarfile.TarFile],
        sha1: Optional[str] = None,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        self.sha1 = sha1
        self.subdirectory = subdirectory
        # self.tarfile_bin = self.get_tarfile()
        # self.get_tarfile()
        # self.tar_dir_name = self.resolve_tar_dir()

    def get_version(self):
        return None

    def nice_version_name(self):
        return "<tarball @ {}>".format(self.tarball)

    def get_tar_size(self):
        return sum(x.size for x in self.tarfile_bin.getmembers())

    def file_sha1(self, fp_in: str):
        if type(fp_in) is str:
            with open(fp_in, 'rb') as fp:
                return hashlib.sha1(fp.read()).hexdigest()
        elif hasattr(fp_in, 'read'):
            return hashlib.sha1(fp_in.read()).hexdigest()
        else:
            raise ValueError()

    def get_tarfile(self):
        def validate_checksum(self, filepath):
            if self.sha1:
                checksum = self.file_sha1(filepath)
                if not checksum == self.sha1:
                    package_sha1_fail(checksum, self.sha1)

        def validate_tarfile(self, filepath):
            if not tarfile.is_tarfile(filepath):
                msg = f"{filepath} is not a valid tarfile."
                raise DependencyException(msg)

        def handle_local_tar(self):
            # pass through local tarfile path primarily for testing
            validate_checksum(self, self.tarball)
            validate_tarfile(self, self.tarball)
            tar = tarfile.open(self.tarball, "r:gz")
            self.tarfile_bin = tar

        def handle_remote_tar(self):
            with NamedTemporaryFile(dir=get_downloads_path()) as named_temp_file:
                download_url = self.tarball

                download_untar_fn = functools.partial(
                    self.download_tar
                    , download_url
                    , named_temp_file.name
                )
                connection_exception_retry(download_untar_fn, 5)

                validate_checksum(self, named_temp_file.name)
                validate_tarfile(self, named_temp_file.name)
                tar = tarfile.open(named_temp_file.name, "r:gz")
                self.tarfile_bin = tar

        if type(self.tarball) is str:
            if os.path.isfile(self.tarball):
                print('handle_local_tar')
                handle_local_tar(self)
            else:
                # assume download if str and not local file
                print('handle_remote_tar')
                handle_remote_tar(self)
        elif type(self.tarball) is tarfile.TarFile:
            # pass through TarFile object primarily for testing
            # no explicit hash test here
            self.tarfile_bin = self.tarball
        else:
            raise ValueError('unknown tarball type defined')

        if not self.get_tar_size() <= TARFILE_MAX_SIZE:
            msg = ("Tarfile size is larger than limit "
                   f"of {TARFILE_MAX_SIZE}.")
            raise DependencyException(msg)

            fire_event(Sha1ChecksumPasses())

    def download_tar(self, download_url, tar_path):
        """
        Sometimes the download of the files fails and we want to retry.  Sometimes the
        download appears successful but the file did not make it through as expected
        (generally due to a github incident).  Either way we want to retry downloading
        and untarring to see if we can get a success.  Call this within
        `_connection_exception_retry`
        """

        system.download(download_url, tar_path)

    def resolve_tar_dir(self):
        ''' Assumed structure:
              Tarfile has one folder in the a root dir 'project folder'.
              This 'project folder' contains the package to install.
              The 'project folder' name is the name of the package.  
              This mirrors the format established for packages on dbt hub.
            Or user can specify subdirectory arg in packages.yaml to manually
              specify folder in tar root to use for install.
              Tarfile has one folder in the a root dir matching subdirectory 
              name.
            '''
        members = self.tarfile_bin.getmembers()
        tarfile_dir_list = [x.name for x in members if x.isdir()]
        if len(tarfile_dir_list) < 1:
            package_structure_malformed()

        if not self.subdirectory:
            tar_dir_name = system.resolve_tar_dir_name(self.tarfile_bin)
            if tar_dir_name == '':
                package_structure_malformed()

        else:
            if self.subdirectory not in tarfile_dir_list:
                msg = (f"{self.subdirectory} is not a valid dir in tarfile.")
                raise DependencyException(msg)

            tar_dir_name = self.subdirectory
            # check that this actually exists in tarfile?

        return tar_dir_name

    def _fetch_metadata(self, project, renderer):
        self.get_tarfile()

        tarfile_bin = self.tarfile_bin
        tar_dir_name = self.resolve_tar_dir()

        fire_event(UntarProjectRoot(
            subdirectory=self.subdirectory,
            tar_dir_name=tar_dir_name))

        tar_path = os.path.realpath(
            os.path.join(get_downloads_path(), tar_dir_name)
        )
        system.make_directory(os.path.dirname(tar_path))

        tarfile_bin.extractall(path=tar_path)

        tar_extracted_root = os.path.join(tar_path, tar_dir_name)

        loaded = Project.from_project_root(tar_extracted_root, renderer)

        return ProjectPackageMetadata.from_project(loaded)

    def install(self, project, renderer):
        self.get_tarfile()

        tar_dir_name = self.resolve_tar_dir()

        tar_path = os.path.realpath(
            os.path.join(get_downloads_path(), tar_dir_name)
        )
        system.make_directory(os.path.dirname(tar_path))
        # tar_extracted_root = os.path.join(tar_path, tar_dir_name)

        # loaded = Project.from_project_root(tar_extracted_root, renderer)

        deps_path = project.packages_install_path
        package_name = self.get_project_name(project, renderer)
        # tarfile_bin.extractall(path=deps_path)

        system.untar_tarfile(TarFile=self.tarfile_bin, dest_dir=deps_path,
                             rename_to=package_name)


class TarballUnpinnedPackage(
    TarballPackageMixin, UnpinnedPackage[TarballPinnedPackage]
):
    def __init__(
        self,
        tarball: str,
        sha1: Optional[str] = None,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        self.sha1 = sha1
        self.subdirectory = subdirectory

    @classmethod
    def from_contract(cls, contract: TarballPackage) -> "TarballUnpinnedPackage":
        return cls(
            tarball=contract.tarball,
            sha1=contract.sha1,
            subdirectory=contract.subdirectory,
        )

    def incorporate(self, other: "TarballUnpinnedPackage") -> "TarballUnpinnedPackage":
        return TarballUnpinnedPackage(
            tarball=self.tarball, sha1=self.sha1, subdirectory=self.subdirectory
        )

    def resolved(self) -> TarballPinnedPackage:
        return TarballPinnedPackage(
            tarball=self.tarball, sha1=self.sha1, subdirectory=self.subdirectory
        )
