import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from app import main

class IngestTests(unittest.TestCase):
    def test_ingest_valid_deduplicates_and_filters(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(main, 'DB', str(Path(tmp) / 'monitor.db')):
                main.prepare_db()
                now=int(time.time())
                event={'Event':'Session-Stop','DestinationID':293,'SourceCall':'S51DG','SourceID':2931234,'RepeaterID':2930001,'Start':now-18,'Stop':now-4}
                self.assertTrue(main.ingest(event, now))
                self.assertFalse(main.ingest(event, now))
                self.assertFalse(main.ingest({**event,'DestinationID':91}, now))
                self.assertFalse(main.ingest({**event,'Event':'Session-Start'}, now))
                self.assertFalse(main.ingest({**event,'Stop':now+1000}, now))
                with main.connect_db() as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM transmissions').fetchone()[0], 1)

if __name__=='__main__': unittest.main()
