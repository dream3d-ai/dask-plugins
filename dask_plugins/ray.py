from distributed import WorkerPlugin, SchedulerPlugin


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

    def __init__(self, ray_address: str, namespace: str | None = None):
        self.ray_address = ray_address
        self.namespace = namespace

    def setup(self, worker):
        import ray

        ray.init(address=self.ray_address, namespace=self.namespace)

    def teardown(self, worker):
        import ray

        ray.shutdown()
