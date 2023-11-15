import hydra
from omegaconf import DictConfig, OmegaConf
from crawler import ChainAbuseScrapper

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg : DictConfig) -> None:
    scraper = ChainAbuseScrapper(**cfg)
    scraper.execute()

if __name__ == "__main__":
    main()