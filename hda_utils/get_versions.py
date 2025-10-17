import platform
import distro
import importlib.metadata

def get_linux_version():
    try:
        return f"{distro.name()} {distro.version()} ({distro.codename()})"
    except Exception:
        return platform.platform()


def get_hda_version():
    try:
        return importlib.metadata.version("hda")
    except importlib.metadata.PackageNotFoundError:
        return "not installed"


def get_versions():
    return {
        "linux_version": get_linux_version(),
        "script_version": "0.0.1",
        "hda_version": get_hda_version(),
    }

if __name__ == "__main__":
    versions = get_versions()
    print(versions)