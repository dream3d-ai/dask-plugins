from distributed import SchedulerPlugin, WorkerPlugin


class RayWorkerPlugin(WorkerPlugin):
    name = "ray"

    def __init__(
        self,
        ray_address: str,
        namespace: str | None = None,
        **kwargs,
    ):
        self.ray_address = ray_address
        self.namespace = namespace
        self.kwargs = kwargs

    def setup(self, worker):
        import ray

        ray.init(
            address=self.ray_address,
            namespace=self.namespace,
            **kwargs,
        )

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
        **kwargs,
    ):
        self.ray_address = ray_address
        self.namespace = namespace
        self.object_store_memory = object_store_memory
        self.kwargs = kwargs

    def setup(self, scheduler):
        import ray

        ray.init(
            address=self.ray_address,
            namespace=self.namespace,
            **kwargs,
        )

    def teardown(self, scheduler):
        import ray

        ray.shutdown()
