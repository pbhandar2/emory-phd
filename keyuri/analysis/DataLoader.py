from keyuri.config.Config import GlobalConfig


class DataLoader:
    def __init__(
            self,
            config: GlobalConfig = GlobalConfig()
    ) -> None:
        self._config = config 
    
    