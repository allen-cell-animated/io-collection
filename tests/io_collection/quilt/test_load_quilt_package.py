import sys
import unittest
from unittest import mock

import quilt3

from io_collection.quilt.load_quilt_package import load_quilt_package


class TestLoadQuiltPackage(unittest.TestCase):
    @mock.patch.object(sys.modules["io_collection.quilt.load_quilt_package"], "quilt3")
    def test_load_quilt_package(self, quilt_mock):
        name = "package_name"
        registry = "package_registry"

        package_mock = mock.MagicMock(spec=quilt3.Package)
        quilt_mock.Package.browse.return_value = package_mock

        package = load_quilt_package(name, registry)

        self.assertEqual(package_mock, package)
        quilt_mock.Package.browse.assert_called_with(name, registry)


if __name__ == "__main__":
    unittest.main()
