import ckan
import ckan.plugins as p
from pylons import config

class MeteoswissHarvest(p.SingletonPlugin):
    """
    Plugin containg the harvester for meteoswiss
    """
