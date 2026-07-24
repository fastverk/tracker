"""rust_service_image — package a rust_binary into a distroless OCI image + push.

Ported from botnoc//tools/oci:defs.bzl (the fastverk house macro). Emits, per call:
  * <name>          oci_image (binary + runfiles at /app on the distroless/cc base)
  * <name>_tarball  oci_load  (→ local docker/podman as <repository>:latest)
  * <name>_push     oci_push  (only when `repository` is set) — the CI target. The
                    build-runner overrides repo+tag at run time:
                      bazel run //services/agent-coord:agent-coord-image_push \
                        --config=rbe -- --repository <ECR>/agent-coord --tag <sha>
                    so the ECR repo == the target basename minus `-image_push`.

There is NO platform transition here: the rust_binary builds for the active
`--platforms`. Cross-compile to linux/amd64 is driven externally — on RBE the build
runs on a linux worker (`--config=rbe` + `--platforms=//tools/oci:linux_amd64`); the
distroless/cc base carries glibc + ca-certs for the zig gnu.2.28 dynamically-linked
binaries (distroless/static would not link).
"""

load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def rust_service_image(
        name,
        binary,
        repository = None,
        exposed_ports = None,
        env = None,
        user = None,
        args = None,
        visibility = None):
    """Package a Rust service `binary` into a distroless_cc OCI image (+ push/load).

    Emits `<name>` (oci_image), `<name>_push` (oci_push), and `<name>_load`
    (oci_load) targets; the binary + runfiles are layered under /app.

    Args:
        name: Base name for the generated image/push/load targets.
        binary: The rust_binary target to containerize.
        repository: Push repository (ECR URI set at push time when None).
        exposed_ports: Container ports to expose (list of "<port>/<proto>").
        env: Extra environment variables (dict).
        user: Runtime user (defaults to the image's).
        args: Default container args.
        visibility: Target visibility.
    """
    exposed_ports = exposed_ports or []
    env = dict(env or {})

    # Layer: binary + runfiles rooted at /app.
    pkg_tar(
        name = name + "_layer",
        srcs = [binary],
        include_runfiles = True,
        package_dir = "/app",
        visibility = ["//visibility:private"],
    )

    binary_name = binary.rsplit(":", 1)[-1] if ":" in binary else binary.rsplit("/", 1)[-1]

    annotations = {}
    if repository:
        annotations["org.opencontainers.image.ref.name"] = repository

    oci_image(
        name = name,
        base = "//tools/oci:base_image",
        entrypoint = ["/app/" + binary_name],
        cmd = args,
        env = env,
        exposed_ports = exposed_ports,
        tars = [name + "_layer"],
        user = str(user) if user != None else None,
        workdir = "/app",
        annotations = annotations,
        visibility = visibility,
    )

    oci_load(
        name = name + "_tarball",
        image = ":" + name,
        repo_tags = [(repository or name) + ":latest"],
        visibility = visibility,
    )

    if repository:
        oci_push(
            name = name + "_push",
            image = ":" + name,
            repository = repository,
            visibility = visibility,
        )
