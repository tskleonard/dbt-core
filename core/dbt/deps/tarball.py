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
        tarball: str,
        sha1: Optional[str] = None,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        self.sha1 = sha1
        self.subdirectory = subdirectory
        self.prep_tarfile()

    def get_version(self):
        return None

    def nice_version_name(self):
        return "<tarball @ {}>".format(self.tarball)

    def get_temp_name(self):
        import tempfile

        return next(tempfile._get_candidate_names())

    def prep_tarfile(self):
        def validate_checksum(self, filepath: str):
            if self.sha1:
                checksum = system.file_sha1(filepath)
                if not checksum == self.sha1:
                    package_sha1_fail(filepath, checksum, self.sha1)
                fire_event(Sha1ChecksumPasses(sha1=self.sha1, filepath=filepath))

        def validate_tarfile(self, filepath: str):
            if not tarfile.is_tarfile(filepath):
                msg = f"{filepath} is not a valid tarfile."
                raise DependencyException(msg)

        # def handle_local_tar(self):
        #     # basic tarfile path passthrough primarily for testing
        #     # validate_checksum(self, self.tarball)
        #     # validate_tarfile(self, self.tarball)
        #     self.tarfile_path = self.tarball

        # def handle_remote_tar(self):
        #     """copied from /dbt/deps/registry.py install
        #     However we can't just package download/untar/install together as one
        #     step like in registry. With registry we can simply untar into the
        #     packages folder. Here, first we need to download and untar to temporary
        #     location to do some validation then discovery on internals of the tarfile."""

        #     tar_name = "{}.tar.gz".format(self.get_temp_name())
        #     tar_path = os.path.realpath(os.path.join(get_downloads_path(), tar_name))
        #     system.make_directory(os.path.dirname(tar_path))

        #     download_url = self.tarball
        #     download_untar_fn = functools.partial(
        #         system.download, download_url, tar_path
        #     )
        #     connection_exception_retry(download_untar_fn, 5)

        #     # validate_checksum(self, tar_path)
        #     # validate_tarfile(self, tar_path)
        #     self.tarfile_path = tar_path

        if os.path.isfile(self.tarball):
            # try local first
            self.tarfile_path = self.tarball
            tarball_type = 'local'
        else:
            # assume download if not local file
            """copied from /dbt/deps/registry.py install
            However we can't just package download/untar/install together as one
            step like in registry. With registry we can simply untar into the
            packages folder. Here, first we need to download and untar to temporary
            location to do some validation then discovery on internals of the tarfile."""

            tar_name = "{}.tar.gz".format(self.get_temp_name())
            tar_path = os.path.realpath(os.path.join(get_downloads_path(), tar_name))
            system.make_directory(os.path.dirname(tar_path))

            download_url = self.tarball
            download_untar_fn = functools.partial(
                system.download, download_url, tar_path
            )
            connection_exception_retry(download_untar_fn, 5)

            # validate_checksum(self, tar_path)
            # validate_tarfile(self, tar_path)
            self.tarfile_path = tar_path
            tarball_type = 'remote'

        validate_checksum(self, self.tarfile_path)
        validate_tarfile(self, self.tarfile_path)
        fire_event(TarballReceivedFeedback(tarball_type=tarball_type))

        """Assumed structure:
          Like registry: package at root of tarfile.
            Example: dbt-utils-0.6.6.tgz -> 
                /dbt-utils/dbt_project.yml
            This is default package format this is expected.
          Like git subdirectory: package is in subdirectory of tarfile.
            Example: packages-tar.tgz -> /packages-folder/dbt-utils-0.6.6/dbt_project.yml
            Use subdirectory setting as `packages-folder/dbt-utils-0.6.6` in this case.
        """
        with tarfile.open(self.tarfile_path, "r:gz") as tarball:
            members = tarball.getmembers()
            tar_size = sum(x.size for x in members)
            if not tar_size <= TARFILE_MAX_SIZE:
                msg = "Tarfile size is larger than limit " f"of {TARFILE_MAX_SIZE}."
                raise DependencyException(msg)

            tarfile_dir_list = [x.name for x in members if x.isdir()]
            # print(tarfile_dir_list)

            # subdirectory arg not given, get root name
            if not self.subdirectory:
                if len(tarfile_dir_list) > 0:
                    resolved_tar_dir_name = os.path.commonpath(tarfile_dir_list)
                    # returns "" if multiple candidates for commonpath
                else:
                    # e.g. if tarfile has files and no parent dir
                    resolved_tar_dir_name = ""

                if resolved_tar_dir_name == "":
                    package_structure_malformed()
                tar_dir_name = resolved_tar_dir_name

            # user gave subdirectory, check if exists
            else:
                # first validate that tarfile has subdirs
                if len(tarfile_dir_list) < 1:
                    package_structure_malformed()
                if self.subdirectory not in tarfile_dir_list:
                    msg = f"{self.subdirectory} is not a valid dir in tarfile."
                    raise DependencyException(msg)
                tar_dir_name = self.subdirectory

        fire_event(
            UntarProjectRoot(subdirectory=self.subdirectory, tar_dir_name=tar_dir_name)
        )
        self.subdirectory = tar_dir_name

        tar_name = self.get_temp_name()
        untar_path = os.path.realpath(os.path.join(get_downloads_path(), tar_name))
        system.make_directory(os.path.dirname(untar_path))
        system.untar_package(self.tarfile_path, untar_path)
        self.untar_path = untar_path

    def _fetch_metadata(self, project, renderer):
        # have an untared folder in temp storage, will pass
        # to metadata for subsequent file checks to be performed
        # on that temp dir
        tar_dir_name = self.subdirectory
        tar_extracted_root = os.path.realpath(
            os.path.join(self.untar_path, tar_dir_name)
        )

        loaded = Project.from_project_root(tar_extracted_root, renderer)

        return ProjectPackageMetadata.from_project(loaded)

    def install(self, project, renderer):
        """copied from /dbt/deps/registry.py install
        We can't just package download/untar/install together as one
        step like in registry. So far we've untarred to a temp location
        scanned the tarfile for folder structure, and determined where
        in the tarfile the package is located, and destination folder
        name in the packages install location. Do a lot of recycling here
        mostly just building out paths for final copy operation. Temp untar
        location + subdir if needed -> package install + dest folder name"""
        tar_name = self.get_temp_name()
        untar_path = os.path.realpath(os.path.join(get_downloads_path(), tar_name))
        system.make_directory(os.path.dirname(untar_path))
        system.untar_package(self.tarfile_path, untar_path)

        tar_dir_name = self.subdirectory
        source_path = os.path.realpath(os.path.join(untar_path, tar_dir_name))

        deps_path = project.packages_install_path
        fire_event(CopyFeedback(source_path=source_path, dest_path=deps_path))

        system.move(source_path, deps_path)


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
