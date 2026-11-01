"""One local engine selection for case deployment and disposable middleware fixtures."""
import os
import shlex


class LocalContainerEngine:
    def __init__(self, override=None):
        command = os.environ.get(override, "") if override else ""
        arguments = shlex.split(command or os.environ.get("CONTAINER_CLI", "docker"))
        if arguments[:2] == ["sudo", "-n"]:
            self.sudo, arguments = True, arguments[2:]
            if arguments[:1] == ["--preserve-env"]:
                arguments = arguments[1:]
        else:
            self.sudo = False
        if arguments not in [["docker"], ["podman"]]:
            raise ValueError("E2E requires a local Docker or Podman engine")
        self.name = arguments[0]

    def arguments(self, preserve_environment=False):
        prefix = []
        if self.sudo:
            prefix = ["sudo", "-n", "--preserve-env" if preserve_environment else
                      "--preserve-env=HTTPS_PROXY,HTTP_PROXY,NO_PROXY,PODMAN_COMPOSE_PROVIDER"]
        return prefix + [self.name] + (["--remote=false"] if self.name == "podman"
                                      else ["--host", "unix:///var/run/docker.sock"])
