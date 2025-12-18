import os

__name__ = "golang"
__author__ = "menduo"

__verbase__ = "0.0.3"
__version__ = __verbase__

__env_vernum__ = os.getenv("MDVERNUM")
__env_verend__ = os.getenv("MDVEREND", "")

if __env_vernum__:
    __version__ = __env_vernum__
else:
    if __env_verend__:
        __version__ = f"0.0.2.{__env_verend__}"
