import quilt3

from prefect import task


@task
def load_quilt_package(quilt_package: str, quilt_registry: str) -> quilt3.Package:
    package = quilt3.Package.browse(quilt_package, quilt_registry)
    return package
