from indexd.index.drivers.query import URLsQueryDriver


class AlchemyURLsQueryDriver(URLsQueryDriver):

    def __init__(self, alchemy_driver):
        """
        Args:
            alchemy_driver (indexd.index.drivers.alchemy.SQLAlchemyIndexDriver):
        """
        self.driver = alchemy_driver
