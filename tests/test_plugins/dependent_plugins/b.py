from plugin_manager import BasePlugin


class B(BasePlugin):
    name = "b"
    depends = ["a"]