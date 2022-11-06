import functools
import os
from typing import Optional

from dbt.clients import system
from dbt.contracts.project import RegistryPackageMetadata, TarballPackage
from dbt.deps.base import PinnedPackage, UnpinnedPackage, get_downloads_path
from dbt.utils import _connection_exception_retry as connection_exception_retry


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
        package: str,
        version: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        # setup to recycle RegistryPinnedPackage fns
        self.package = package
        self.version = version

    @property
    def name(self):
        return self.package

    def get_version(self):
        return self.version

    def nice_version_name(self):
        return "version {}".format(self.version)

    def _fetch_metadata(self, project, renderer):
        """
        recycle RegistryPackageMetadata so that we can use the install and
        download_and_untar from RegistryPinnedPackage next.
        build RegistryPackageMetadata from info passed via packages.yml since no
        'metadata' service exists in this case.
        """
        if self.version:
            name = "{}.{}".format(self.package, self.version)
        else:
            name = self.package

        dct = {
            "name": name,
            "packages": [],  # note: required by RegistryPackageMetadata
            "downloads": {"tarball": self.tarball},
        }

        return RegistryPackageMetadata.from_dict(dct)

    def install(self, project, renderer):
        """
        verbatim from dbt.deps.registry.RegistryPinnedPackage
            install() from RegistryPinnedPackage class
            dbt crew to refactor this to more general place so it can be reused here?
        this should be moved to dbt.deps.base, or to a dbt.deps.common file, waiting on 
        dbt labs feedback on how to proceed (or leave as is)
        """
        metadata = self.fetch_metadata(project, renderer)

        tar_name = "{}.{}.tar.gz".format(self.package, self.version)
        tar_path = os.path.realpath(os.path.join(get_downloads_path(), tar_name))
        system.make_directory(os.path.dirname(tar_path))

        download_url = metadata.downloads.tarball
        deps_path = project.packages_install_path
        package_name = self.get_project_name(project, renderer)

        download_untar_fn = functools.partial(
            self.download_and_untar, download_url, tar_path, deps_path, package_name
        )
        connection_exception_retry(download_untar_fn, 5)

    def download_and_untar(self, download_url, tar_path, deps_path, package_name):
        """
        verbatim from dbt.deps.registry.RegistryPinnedPackage
            download_and_untar() from RegistryPinnedPackage class
            dbt crew to refactor this to more general place so it can be reused here?
        this should be moved to dbt.deps.base, or to a dbt.deps.common file, waiting on 
        dbt labs feedback on how to proceed (or leave as is)
        """

        """
        Sometimes the download of the files fails and we want to retry.  Sometimes the
        download appears successful but the file did not make it through as expected
        (generally due to a github incident).  Either way we want to retry downloading
        and untarring to see if we can get a success.  Call this within
        `_connection_exception_retry`
        """

        system.download(download_url, tar_path)
        system.untar_package(tar_path, deps_path, package_name)


class TarballUnpinnedPackage(TarballPackageMixin, UnpinnedPackage[TarballPinnedPackage]):
    def __init__(
        self,
        tarball: str,
        package: str,
        version: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        # setup to recycle RegistryPinnedPackage fns
        self.package = package
        self.version = version

    @classmethod
    def from_contract(cls, contract: TarballPackage) -> "TarballUnpinnedPackage":
        return cls(
            tarball=contract.tarball,
            package=contract.name,
            version=contract.version,
        )

    def incorporate(self, other: "TarballUnpinnedPackage") -> "TarballUnpinnedPackage":
        return TarballUnpinnedPackage(
            tarball=self.tarball, package=self.package, version=self.version
        )

    def resolved(self) -> TarballPinnedPackage:
        return TarballPinnedPackage(
            tarball=self.tarball, package=self.package, version=self.version
        )
