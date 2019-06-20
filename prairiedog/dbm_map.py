import dbm
import logging
import os

import prairiedog.config

log = logging.getLogger("prairiedog")

DBM_LOCATION = os.path.join(
        prairiedog.config.OUTPUT_DIRECTORY, 'edge_map.db')

log.info("Initializing DBMMap with backing file {}".format(DBM_LOCATION))


def _upsert(db, key: str) -> int:
    if key not in db:
        if 'mx' not in db:
            mx = -1
            db['mx'] = str(mx)
        else:
            mx = int(db['mx'])
        mx += 1
        db[key] = str(mx)
        db['mx'] = str(mx)
        return mx
    else:
        return int(db[key])


d = {}


def upsert(key: str, backing: str = 'dbm') -> int:
    if backing == 'dbm':
        with dbm.open(DBM_LOCATION, 'c') as db:
            return _upsert(db, key)
    else:
        return _upsert(d, key)
