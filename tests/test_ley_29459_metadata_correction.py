import json
import unittest
from pathlib import Path

from scripts import seed_digemid_normas_minimas as seed


ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "data" / "normativa_seed_minima.json"
SQL_PATH = ROOT / "supabase" / "sql" / "2026_10_26_correct_ley_29459_dates.sql"


class Ley29459MetadataCorrectionTest(unittest.TestCase):
    def test_seed_preserves_correct_dates_and_official_audit_source(self):
        records = json.loads(SEED_PATH.read_text(encoding="utf-8"))
        ley = next(item for item in records if item["document_key"] == "LEY-29459")

        self.assertEqual(ley["fecha_promulgacion"], "2009-11-25")
        self.assertEqual(ley["fecha_publicacion"], "2009-11-26")
        self.assertIn("fecha_promulgacion", seed.SAFE_UPDATE_FIELDS)
        self.assertIn("fuente oficial", ley["raw"]["metadata_correction"]["reason"])
        self.assertEqual(
            ley["raw"]["metadata_correction"]["source_url"],
            ley["source_url"],
        )
        merged = seed.merge_raw({}, ley["raw"], ley["document_key"])
        self.assertEqual(
            merged["seed"]["metadata_correction"],
            ley["raw"]["metadata_correction"],
        )

    def test_sql_correction_is_guarded_idempotent_and_narrow(self):
        sql = SQL_PATH.read_text(encoding="utf-8").lower()
        set_clause = sql.split("set", 1)[1].split("from correction", 1)[0]

        self.assertIn("where norma.document_key = 'ley-29459'", sql)
        self.assertIn("fecha_promulgacion = date '2009-11-25'", sql)
        self.assertIn("fecha_publicacion = date '2009-11-26'", sql)
        self.assertIn("is distinct from", sql)
        self.assertIn("metadata_corrections", sql)
        self.assertNotIn("source_url =", set_clause)
        self.assertNotIn("pdf_url", set_clause)
        self.assertNotIn("estado_vigencia", set_clause)


if __name__ == "__main__":
    unittest.main()
