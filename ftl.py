class FTL:
    def warp(self):
        if hasattr(self, "fetch"):
            self.fetch()
        if hasattr(self, "transform"):
            self.transform()
        if hasattr(self, "load"):
            self.load()
