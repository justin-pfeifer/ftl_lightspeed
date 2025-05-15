
class FTL:
    def warp(self):
        if hasattr(self, "fetch"):
            self.fetch()
        if hasattr(self, "transform"):
            self.transform()
        if hasattr(self, "load"):
            self.load()

    def getJobName(self) -> str:
        # Converts PascalCase class name → readable name
        return self.__class__.__name__
