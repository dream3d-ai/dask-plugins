from distributed import SchedulerPlugin, WorkerPlugin


class RayWorkerPlugin(WorkerPlugin):
    name = "ray"

    def __init__(self, ray_address: str, namespace: str | None = None):
        self.ray_address = ray_address
        self.namespace = namespace

    def setup(self, worker):
        import ray

        ray.init(address=self.ray_address, namespace=self.namespace)

    def teardown(self, worker):
        import ray

        ray.shutdown()


class RaySchedulerPlugin(SchedulerPlugin):
    name = "ray"

    def __init__(
        self,
        ray_address: str,
        packages: str | None = None,
        namespace: str | None = None,
    ):
        self.ray_address = ray_address
        self.namespace = namespace

        self.packages = NOne
        if packages is not None:
            with open(packages, "r") as f:
                self.packages = f.read()

    def setup(self, scheduler):
        import ray

        runtime_env = None
        if self.packages is not None:
            import tempfile
            import zipfile

            with tempfile.NamedTemporaryFile() as f:
                f.write(self.packages)
                f.flush()
                with zipfile.ZipFile(f.name, "r") as zip_ref:
                    zip_ref.extractall("/tmp/user-packages")
            runtime_env = {"working_dir": "/tmp/user-packages"}

        ray.init(
            address=self.ray_address,
            namespace=self.namespace,
            runtime_env=runtime_env,
        )

    def teardown(self, scheduler):
        import ray

        ray.shutdown()
