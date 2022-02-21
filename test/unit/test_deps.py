import unittest
from unittest import mock

import dbt.deps
import dbt.exceptions
from dbt.deps.git import GitUnpinnedPackage
from dbt.deps.local import LocalUnpinnedPackage
from dbt.deps.tarball import TarballUnpinnedPackage, TARFILE_MAX_SIZE
from dbt.deps.registry import RegistryUnpinnedPackage
from dbt.deps.resolver import resolve_packages
from dbt.contracts.project import (
    LocalPackage,
    TarballPackage,
    GitPackage,
    RegistryPackage,
)

from dbt.contracts.project import PackageConfig
from dbt.semver import VersionSpecifier

from dbt.dataclass_schema import ValidationError


class TestLocalPackage(unittest.TestCase):
    def test_init(self):
        a_contract = LocalPackage.from_dict({'local': '/path/to/package'})
        self.assertEqual(a_contract.local, '/path/to/package')
        a = LocalUnpinnedPackage.from_contract(a_contract)
        self.assertEqual(a.local, '/path/to/package')
        a_pinned = a.resolved()
        self.assertEqual(a_pinned.local, '/path/to/package')
        self.assertEqual(str(a_pinned), '/path/to/package')


class TestTarballPackage(unittest.TestCase):
    import tarfile

    def mock_tarball(self, def_in):
        import tarfile
        # helper to make tarfile in memory for subseqent testing
        import io

        with io.BytesIO() as f:
            tar = tarfile.open(fileobj=f, mode="x")
            for d in def_in:
                t = tarfile.TarInfo(d["name"])
                if d.get("type"):
                    t.type = d.get("type")
                if d.get("size"):
                    t.size = d.get("size")
                tar.addfile(t)
        return tar

    bad_tar_def = [{"size": 1000, "name": "my_file.py"}]
    bad_tar_def_sz = [
        {"type": tarfile.DIRTYPE, "name": "dir_1"},
        {"size": TARFILE_MAX_SIZE + 1, "name": "dir_1/my_file.py"},
    ]
    good_tar_def = [
        {"type": tarfile.DIRTYPE, "name": "dir_1"},
        {"size": 1000, "name": "dir_1/my_file.py"},
    ]
    many_dir_dar_def = [
        {"type": tarfile.DIRTYPE, "name": "dir_1"},
        {"type": tarfile.DIRTYPE, "name": "dir_2"},
        {"size": 1000, "name": "dir_1/my_file.py"},
    ]

    def test_init(self):
        a_contract = TarballPackage.from_dict(
            {"tarball": "http://example.com"})
        self.assertEqual(a_contract.tarball, "http://example.com")
        a = TarballUnpinnedPackage.from_contract(a_contract)
        self.assertEqual(a.tarball, "http://example.com")

    def test_init_tarfiles(self):
        mock_good_tar = self.mock_tarball(self.good_tar_def)
        mock_bad_tar = self.mock_tarball(self.bad_tar_def)

        a_contract = TarballPackage.from_dict({"tarball": mock_good_tar})
        a = TarballUnpinnedPackage.from_contract(a_contract)
        a_pinned = a.resolved()
        self.assertEqual(a_pinned.tarfile_bin, mock_good_tar)

        a_contract = TarballPackage.from_dict({"tarball": mock_bad_tar})
        a = TarballUnpinnedPackage.from_contract(a_contract)
        with self.assertRaises(dbt.exceptions.DependencyException):
            a_pinned = a.resolved()

    def test_tarfile_args(self):
        mock_bad_tar = self.mock_tarball(self.bad_tar_def)
        mock_bad_tar_sz = self.mock_tarball(self.bad_tar_def_sz)
        mock_many_dir_tar = self.mock_tarball(self.many_dir_dar_def)
        mock_good_tar = self.mock_tarball(self.good_tar_def)

        a_contract = TarballPackage.from_dict(
            {"tarball": "http://example.com",
             "sha1": "123", "subdirectory": "subdir"}
        )
        self.assertEqual(a_contract.tarball, "http://example.com")
        self.assertEqual(a_contract.sha1, "123")
        self.assertEqual(a_contract.subdirectory, "subdir")

        a = TarballUnpinnedPackage.from_contract(a_contract)
        self.assertEqual(a.tarball, "http://example.com")
        self.assertEqual(a.sha1, "123")
        self.assertEqual(a.subdirectory, "subdir")

        a_contract = TarballPackage.from_dict(
            {"tarball": mock_good_tar, "sha1": "123", "subdirectory": "dir_1"}
        )
        a = TarballUnpinnedPackage.from_contract(a_contract)
        a_pinned = a.resolved()
        self.assertEqual(a_pinned.tarfile_bin, mock_good_tar)
        self.assertEqual(a_pinned.sha1, "123")
        self.assertEqual(a_pinned.subdirectory, "dir_1")
        self.assertEqual(a_pinned.resolve_tar_dir(), "dir_1")

        a_contract = TarballPackage.from_dict(
            {"tarball": mock_good_tar, "sha1": "123"})
        a = TarballUnpinnedPackage.from_contract(a_contract)
        a_pinned = a.resolved()
        self.assertEqual(a_pinned.tarfile_bin, mock_good_tar)
        self.assertEqual(a_pinned.sha1, "123")
        self.assertEqual(a_pinned.subdirectory, None)
        self.assertEqual(a_pinned.resolve_tar_dir(), "dir_1")

        a_contract = TarballPackage.from_dict(
            {"tarball": mock_many_dir_tar, "subdirectory": "subdir"}
        )
        a = TarballUnpinnedPackage.from_contract(a_contract)
        with self.assertRaises(dbt.exceptions.DependencyException):
            a.resolved()

        a_contract = TarballPackage.from_dict({"tarball": mock_many_dir_tar})
        a = TarballUnpinnedPackage.from_contract(a_contract)
        with self.assertRaises(dbt.exceptions.DependencyException):
            a.resolved()

        a_contract = TarballPackage.from_dict({"tarball": mock_bad_tar_sz})
        a = TarballUnpinnedPackage.from_contract(a_contract)
        with self.assertRaises(dbt.exceptions.DependencyException):
            a.resolved()

    def test_file_obj_sha1(self):
        from dbt.deps.base import get_downloads_path
        import io
        import tarfile
        from tempfile import NamedTemporaryFile
        
        # Test off in memory file object.
        #  In memory sha1, seems stable.
        with io.BytesIO() as f:
            tar = tarfile.open(fileobj=f, mode='w:gz')
            for d in self.good_tar_def:
                t = tarfile.TarInfo(d['name'])
                if d.get('type'):
                    t.type = d.get('type')
                if d.get('size'):
                    t.size = d.get('size')
                tar.addfile(t)

            a_contract = (
                TarballPackage.from_dict({'tarball': tar}))
            a = TarballUnpinnedPackage.from_contract(a_contract)
            a_pinned = a.resolved()
            mock_hash = a_pinned.file_sha1(f)
            # in memory sha1, seems stable
            self.assertEqual(mock_hash, 'da39a3ee5e6b4b0d3255bfef95601890afd80709')
            
        # Test off disk write.
        with NamedTemporaryFile(dir=get_downloads_path()) as named_temp_file:
            tar = tarfile.open(named_temp_file.name, mode='w:gz')
            for d in self.good_tar_def:
                t = tarfile.TarInfo(d['name'])
                if d.get('type'):
                    t.type = d.get('type')
                if d.get('size'):
                    t.size = d.get('size')
                tar.addfile(t)
            tar.close()
            
            a_contract = (
                TarballPackage.from_dict({'tarball': named_temp_file.name}))
            a = TarballUnpinnedPackage.from_contract(a_contract)
            a_pinned = a.resolved()
            # Temp file sha1 varies due to path. Extra steps to calculate 
            #  sha1, and pass to second contract, to compare.
            this_hash = a_pinned.file_sha1(named_temp_file.name)
            b_contract = (
                TarballPackage.from_dict(
                    {'tarball': named_temp_file.name,
                     'sha1' : this_hash}
                ))
            b = TarballUnpinnedPackage.from_contract(b_contract)
            self.assertEqual(this_hash, b.sha1)
            

class TestGitPackage(unittest.TestCase):
    def test_init(self):
        a_contract = GitPackage.from_dict(
            {'git': 'http://example.com', 'revision': '0.0.1'},
        )
        self.assertEqual(a_contract.git, 'http://example.com')
        self.assertEqual(a_contract.revision, '0.0.1')
        self.assertIs(a_contract.warn_unpinned, None)

        a = GitUnpinnedPackage.from_contract(a_contract)
        self.assertEqual(a.git, 'http://example.com')
        self.assertEqual(a.revisions, ['0.0.1'])
        self.assertIs(a.warn_unpinned, True)

        a_pinned = a.resolved()
        self.assertEqual(a_pinned.name, 'http://example.com')
        self.assertEqual(a_pinned.get_version(), '0.0.1')
        self.assertEqual(a_pinned.source_type(), 'git')
        self.assertIs(a_pinned.warn_unpinned, True)

    def test_invalid(self):
        with self.assertRaises(ValidationError):
            GitPackage.validate(
                {'git': 'http://example.com', 'version': '0.0.1'},
            )

    def test_resolve_ok(self):
        a_contract = GitPackage.from_dict(
            {'git': 'http://example.com', 'revision': '0.0.1'},
        )
        b_contract = GitPackage.from_dict(
            {'git': 'http://example.com', 'revision': '0.0.1',
             'warn-unpinned': False},
        )
        a = GitUnpinnedPackage.from_contract(a_contract)
        b = GitUnpinnedPackage.from_contract(b_contract)
        self.assertTrue(a.warn_unpinned)
        self.assertFalse(b.warn_unpinned)
        c = a.incorporate(b)

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.name, 'http://example.com')
        self.assertEqual(c_pinned.get_version(), '0.0.1')
        self.assertEqual(c_pinned.source_type(), 'git')
        self.assertFalse(c_pinned.warn_unpinned)

    def test_resolve_fail(self):
        a_contract = GitPackage.from_dict(
            {'git': 'http://example.com', 'revision': '0.0.1'},
        )
        b_contract = GitPackage.from_dict(
            {'git': 'http://example.com', 'revision': '0.0.2'},
        )
        a = GitUnpinnedPackage.from_contract(a_contract)
        b = GitUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)
        self.assertEqual(c.git, 'http://example.com')
        self.assertEqual(c.revisions, ['0.0.1', '0.0.2'])

        with self.assertRaises(dbt.exceptions.DependencyException):
            c.resolved()

    def test_default_revision(self):
        a_contract = GitPackage.from_dict({'git': 'http://example.com'})
        self.assertEqual(a_contract.revision, None)
        self.assertIs(a_contract.warn_unpinned, None)

        a = GitUnpinnedPackage.from_contract(a_contract)
        self.assertEqual(a.git, 'http://example.com')
        self.assertEqual(a.revisions, [])
        self.assertIs(a.warn_unpinned, True)

        a_pinned = a.resolved()
        self.assertEqual(a_pinned.name, 'http://example.com')
        self.assertEqual(a_pinned.get_version(), 'HEAD')
        self.assertEqual(a_pinned.source_type(), 'git')
        self.assertIs(a_pinned.warn_unpinned, True)


class TestHubPackage(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch('dbt.deps.registry.registry')
        self.registry = self.patcher.start()
        self.index_cached = self.registry.index_cached
        self.get_available_versions = self.registry.get_available_versions
        self.package_version = self.registry.package_version

        self.index_cached.return_value = [
            'dbt-labs-test/a',
        ]
        self.get_available_versions.return_value = [
            '0.1.2', '0.1.3', '0.1.4a1'
        ]
        self.package_version.return_value = {
            'id': 'dbt-labs-test/a/0.1.2',
            'name': 'a',
            'version': '0.1.2',
            'packages': [],
            '_source': {
                'blahblah': 'asdfas',
            },
            'downloads': {
                'tarball': 'https://example.com/invalid-url!',
                'extra': 'field',
            },
            'newfield': ['another', 'value'],
        }

    def tearDown(self):
        self.patcher.stop()

    def test_init(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.2',
        )
        self.assertEqual(a_contract.package, 'dbt-labs-test/a')
        self.assertEqual(a_contract.version, '0.1.2')

        a = RegistryUnpinnedPackage.from_contract(a_contract)
        self.assertEqual(a.package, 'dbt-labs-test/a')
        self.assertEqual(
            a.versions,
            [VersionSpecifier(
                build=None,
                major='0',
                matcher='=',
                minor='1',
                patch='2',
                prerelease=None
            )]
        )

        a_pinned = a.resolved()
        self.assertEqual(a_contract.package, 'dbt-labs-test/a')
        self.assertEqual(a_contract.version, '0.1.2')
        self.assertEqual(a_pinned.source_type(), 'hub')

    def test_invalid(self):
        with self.assertRaises(ValidationError):
            RegistryPackage.validate(
                {'package': 'namespace/name', 'key': 'invalid'},
            )

    def test_resolve_ok(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.2'
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.2'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        self.assertEqual(c.package, 'dbt-labs-test/a')
        self.assertEqual(
            c.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='=',
                    minor='1',
                    patch='2',
                    prerelease=None,
                ),
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='=',
                    minor='1',
                    patch='2',
                    prerelease=None,
                ),
            ]
        )

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(c_pinned.version, '0.1.2')
        self.assertEqual(c_pinned.source_type(), 'hub')

    def test_resolve_missing_package(self):
        a = RegistryUnpinnedPackage.from_contract(RegistryPackage(
            package='dbt-labs-test/b',
            version='0.1.2'
        ))
        with self.assertRaises(dbt.exceptions.DependencyException) as exc:
            a.resolved()

        msg = 'Package dbt-labs-test/b was not found in the package index'
        self.assertEqual(msg, str(exc.exception))

    def test_resolve_missing_version(self):
        a = RegistryUnpinnedPackage.from_contract(RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.4'
        ))

        with self.assertRaises(dbt.exceptions.DependencyException) as exc:
            a.resolved()
        msg = (
            "Could not find a matching version for package "
            "dbt-labs-test/a\n  Requested range: =0.1.4, =0.1.4\n  "
            "Available versions: ['0.1.2', '0.1.3']"
        )
        self.assertEqual(msg, str(exc.exception))

    def test_resolve_conflict(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.2'
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.3'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        with self.assertRaises(dbt.exceptions.DependencyException) as exc:
            c.resolved()
        msg = (
            "Version error for package dbt-labs-test/a: Could not "
            "find a satisfactory version from options: ['=0.1.2', '=0.1.3']"
        )
        self.assertEqual(msg, str(exc.exception))

    def test_resolve_ranges(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.2'
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='<0.1.4'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        self.assertEqual(c.package, 'dbt-labs-test/a')
        self.assertEqual(
            c.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='=',
                    minor='1',
                    patch='2',
                    prerelease=None,
                ),
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='<',
                    minor='1',
                    patch='4',
                    prerelease=None,
                ),
            ]
        )

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(c_pinned.version, '0.1.2')
        self.assertEqual(c_pinned.source_type(), 'hub')

    def test_resolve_ranges_install_prerelease_default_false(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='>0.1.2'
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='<0.1.5'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        self.assertEqual(c.package, 'dbt-labs-test/a')
        self.assertEqual(
            c.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='>',
                    minor='1',
                    patch='2',
                    prerelease=None,
                ),
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='<',
                    minor='1',
                    patch='5',
                    prerelease=None,
                ),
            ]
        )

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(c_pinned.version, '0.1.3')
        self.assertEqual(c_pinned.source_type(), 'hub')

    def test_resolve_ranges_install_prerelease_true(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='>0.1.2',
            install_prerelease=True
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='<0.1.5'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        self.assertEqual(c.package, 'dbt-labs-test/a')
        self.assertEqual(
            c.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='>',
                    minor='1',
                    patch='2',
                    prerelease=None,
                ),
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='<',
                    minor='1',
                    patch='5',
                    prerelease=None,
                ),
            ]
        )

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(c_pinned.version, '0.1.4a1')
        self.assertEqual(c_pinned.source_type(), 'hub')

    def test_get_version_latest_prelease_true(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='>0.1.0',
            install_prerelease=True
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='<0.1.4'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        self.assertEqual(c.package, 'dbt-labs-test/a')
        self.assertEqual(
            c.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='>',
                    minor='1',
                    patch='0',
                    prerelease=None,
                ),
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='<',
                    minor='1',
                    patch='4',
                    prerelease=None,
                ),
            ]
        )

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(c_pinned.version, '0.1.3')
        self.assertEqual(c_pinned.get_version_latest(), '0.1.4a1')
        self.assertEqual(c_pinned.source_type(), 'hub')

    def test_get_version_latest_prelease_false(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='>0.1.0',
            install_prerelease=False
        )
        b_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='<0.1.4'
        )
        a = RegistryUnpinnedPackage.from_contract(a_contract)
        b = RegistryUnpinnedPackage.from_contract(b_contract)
        c = a.incorporate(b)

        self.assertEqual(c.package, 'dbt-labs-test/a')
        self.assertEqual(
            c.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='>',
                    minor='1',
                    patch='0',
                    prerelease=None,
                ),
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='<',
                    minor='1',
                    patch='4',
                    prerelease=None,
                ),
            ]
        )

        c_pinned = c.resolved()
        self.assertEqual(c_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(c_pinned.version, '0.1.3')
        self.assertEqual(c_pinned.get_version_latest(), '0.1.3')
        self.assertEqual(c_pinned.source_type(), 'hub')

    def test_get_version_prerelease_explicitly_requested(self):
        a_contract = RegistryPackage(
            package='dbt-labs-test/a',
            version='0.1.4a1',
            install_prerelease=None
        )

        a = RegistryUnpinnedPackage.from_contract(a_contract)

        self.assertEqual(a.package, 'dbt-labs-test/a')
        self.assertEqual(
            a.versions,
            [
                VersionSpecifier(
                    build=None,
                    major='0',
                    matcher='=',
                    minor='1',
                    patch='4',
                    prerelease='a1',
                ),
            ]
        )

        a_pinned = a.resolved()
        self.assertEqual(a_pinned.package, 'dbt-labs-test/a')
        self.assertEqual(a_pinned.version, '0.1.4a1')
        self.assertEqual(a_pinned.get_version_latest(), '0.1.4a1')
        self.assertEqual(a_pinned.source_type(), 'hub')

class MockRegistry:
    def __init__(self, packages):
        self.packages = packages

    def index_cached(self, registry_base_url=None):
        return sorted(self.packages)

    def get_available_versions(self, name):
        try:
            pkg = self.packages[name]
        except KeyError:
            return []
        return list(pkg)

    def package_version(self, name, version):
        try:
            return self.packages[name][version]
        except KeyError:
            return None


class TestPackageSpec(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch('dbt.deps.registry.registry')
        self.registry = self.patcher.start()
        self.mock_registry = MockRegistry(packages={
            'dbt-labs-test/a': {
                '0.1.2': {
                    'id': 'dbt-labs-test/a/0.1.2',
                    'name': 'a',
                    'version': '0.1.2',
                    'packages': [],
                    '_source': {
                        'blahblah': 'asdfas',
                    },
                    'downloads': {
                        'tarball': 'https://example.com/invalid-url!',
                        'extra': 'field',
                    },
                    'newfield': ['another', 'value'],
                },
                '0.1.3': {
                    'id': 'dbt-labs-test/a/0.1.3',
                    'name': 'a',
                    'version': '0.1.3',
                    'packages': [],
                    '_source': {
                        'blahblah': 'asdfas',
                    },
                    'downloads': {
                        'tarball': 'https://example.com/invalid-url!',
                        'extra': 'field',
                    },
                    'newfield': ['another', 'value'],
                }
            },
            'dbt-labs-test/b': {
                '0.2.1': {
                    'id': 'dbt-labs-test/b/0.2.1',
                    'name': 'b',
                    'version': '0.2.1',
                    'packages': [{'package': 'dbt-labs-test/a', 'version': '>=0.1.3'}],
                    '_source': {
                        'blahblah': 'asdfas',
                    },
                    'downloads': {
                        'tarball': 'https://example.com/invalid-url!',
                        'extra': 'field',
                    },
                    'newfield': ['another', 'value'],
                },
            }
        })

        self.registry.index_cached.side_effect = self.mock_registry.index_cached
        self.registry.get_available_versions.side_effect = self.mock_registry.get_available_versions
        self.registry.package_version.side_effect = self.mock_registry.package_version

    def tearDown(self):
        self.patcher.stop()

    def test_dependency_resolution(self):
        package_config = PackageConfig.from_dict({
            'packages': [
                {'package': 'dbt-labs-test/a', 'version': '>0.1.2'},
                {'package': 'dbt-labs-test/b', 'version': '0.2.1'},
            ],
        })
        resolved = resolve_packages(package_config.packages, mock.MagicMock(project_name='test'))
        self.assertEqual(len(resolved), 2)
        self.assertEqual(resolved[0].name, 'dbt-labs-test/a')
        self.assertEqual(resolved[0].version, '0.1.3')
        self.assertEqual(resolved[1].name, 'dbt-labs-test/b')
        self.assertEqual(resolved[1].version, '0.2.1')
