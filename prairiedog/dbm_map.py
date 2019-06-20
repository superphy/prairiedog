import dbm
import logging
import os

import prairiedog.config

log = logging.getLogger("prairiedog")

DBM_LOCATION = os.path.join(
        prairiedog.config.OUTPUT_DIRECTORY, 'edge_map.db')

log.info("Initializing DBMMap with backing file {}".format(DBM_LOCATION))


def upsert(key: str) -> int:
    with dbm.open(DBM_LOCATION, 'c') as db:
        if key not in db:
            if 'mx' not in db:
                db['mx'] = str(-1)
                mx = -1
            else:
                mx = int(db['mx'])
            mx += 1
            db[key] = str(mx)
            db['mx'] = str(mx)
            return mx
        else:
            return int(db[key])
