import quilt3


def load_quilt_package(quilt_package: str, quilt_registry: str) -> quilt3.Package:
    """
    Load Quilt package into memory from registry.

    Package is loaded without making a local copy of the manifest.

    Parameters
    ----------
    quilt_package
        Quilt package name, formatted as {namespace}/{packagename}.
    quilt_registry
        Package registry name.

    Returns
    -------
    :
        The Quilt package.
    """

    return quilt3.Package.browse(quilt_package, quilt_registry)
