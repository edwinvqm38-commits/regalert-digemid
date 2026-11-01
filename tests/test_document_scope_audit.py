import unittest

from agents.document_scope_audit import (
    MIXED_PENDING_REVIEW,
    SCOPE_UNKNOWN,
    SCOPE_VERIFIED,
    ScopeBlock,
    ScopePage,
    ScopeTable,
    audit_document_scope,
    audit_page_scope,
    parse_document_key,
    render_scope_summary,
)


def block(text, x0, y0, x1, y1):
    return ScopeBlock(text=text, bbox=(x0, y0, x1, y1))


def table(y0, y1, rows, text="", x0=20, x1=580):
    return ScopeTable(bbox=(x0, y0, x1, y1), rows=rows, text=text)


def audit(document_key, pages):
    return audit_document_scope(
        document_key=document_key,
        pdf_name="fixture_sintetico.pdf",
        pdf_sha256="synthetic-fixture-not-a-real-pdf",
        source_asset_id=77,
        pages=pages,
        evidence_source="synthetic_layout_fixture",
    )


class DocumentScopeAuditTest(unittest.TestCase):
    def test_ds_5_2019_page_1_flags_three_foreign_tables(self):
        page = ScopePage(
            page_number=1,
            printed_page="35",
            width=600,
            height=800,
            blocks=[
                block("MINISTERIO DE RELACIONES EXTERIORES", 20, 30, 285, 55),
                block("RESOLUCIÓN MINISTERIAL N° 0120-2019-RE", 20, 60, 285, 95),
                block("DECRETO SUPREMO N° 005-2019-SA", 320, 620, 580, 650),
                block(
                    "EL PRESIDENTE DE LA REPÚBLICA\nCONSIDERANDO:", 320, 655, 580, 740
                ),
            ],
            tables=[
                table(105, 230, [["Funcionario", "Viáticos"], ["A", "100"]], x1=285),
                table(240, 390, [["Funcionario", "Viáticos"], ["B", "200"]], x1=285),
                table(400, 570, [["Funcionario", "Viáticos"], ["C", "300"]], x1=285),
            ],
        )
        report = audit("DS-5-2019-SA", [page])
        result = report["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertEqual(result["page_number_printed"], "35")
        suspicious_tables = [
            item for item in result["suspicious_regions"] if item["kind"] == "table"
        ]
        self.assertEqual(len(suspicious_tables), 3)
        self.assertEqual(report["pdf_name"], "fixture_sintetico.pdf")
        self.assertEqual(report["source_asset_id"], 77)
        self.assertEqual(report["summary"]["writes_performed"], 0)
        self.assertIn("mixed_pending_review", render_scope_summary(report))

    def test_ds_5_2019_page_12_annex_table_belongs(self):
        page = ScopePage(
            page_number=12,
            printed_page="46",
            width=600,
            height=800,
            blocks=[
                block(
                    "ANEXO N° 02 DEL DECRETO SUPREMO N° 005-2019-SA\n"
                    "Causales de suspensión o cancelación de licencias",
                    40,
                    40,
                    560,
                    105,
                )
            ],
            tables=[
                table(
                    120,
                    740,
                    [["N°", "Causal"]]
                    + [[str(i), f"Causal {i}"] for i in range(1, 14)],
                )
            ],
        )
        result = audit("DS-5-2019-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertFalse(result["tables"][0]["suspicious_scope"])
        self.assertFalse(result["review_pending"])

    def test_ds_5_2019_page_13_possible_transition(self):
        page = ScopePage(
            page_number=13,
            width=600,
            height=800,
            blocks=[
                block("Fin del Anexo N° 02", 30, 30, 570, 180),
                block("RESOLUCIÓN MINISTERIAL N° 251-2019-MINSA", 30, 520, 570, 555),
                block("SE RESUELVE:", 30, 570, 570, 600),
            ],
        )
        result = audit("DS-5-2019-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertTrue(result["review_pending"])

    def test_rm_250_2019_page_1_flags_previous_disposition(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("RESOLUCIÓN MINISTERIAL N° 249-2019-MINSA", 30, 25, 570, 60),
                block("Regístrese, comuníquese y publíquese.", 30, 65, 570, 150),
                block("RESOLUCIÓN MINISTERIAL N° 250-2019-MINSA", 30, 430, 570, 470),
                block("Lima, 15 de marzo de 2019\nCONSIDERANDO:", 30, 480, 570, 600),
            ],
        )
        result = audit("RM-250-2019-MINSA", [page])["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        codes = {signal["code"] for signal in result["signals"]}
        self.assertIn("foreign_norm_heading", codes)
        self.assertIn("expected_document_starts_late", codes)

    def test_simple_document_is_verified_only_with_positive_identity(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO N° 005-2019-SA", 30, 30, 570, 65),
                block("Artículo 1.- Objeto del Decreto Supremo.", 30, 100, 570, 180),
            ],
        )
        self.assertEqual(
            audit("DS-5-2019-SA", [page])["pages"][0]["classification"], SCOPE_VERIFIED
        )

        unknown = ScopePage(
            page_number=2,
            width=600,
            height=800,
            blocks=[
                block(
                    "Texto aislado sin identidad documental suficiente.",
                    30,
                    30,
                    570,
                    100,
                )
            ],
        )
        self.assertEqual(
            audit("DS-5-2019-SA", [unknown])["pages"][0]["classification"],
            SCOPE_UNKNOWN,
        )

    def test_legitimate_legal_reference_is_not_foreign_content(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO N° 005-2019-SA", 30, 30, 570, 65),
                block(
                    "Artículo 2.- De conformidad con la Ley N° 29459-2009, se establecen requisitos.",
                    30,
                    100,
                    570,
                    190,
                ),
            ],
        )
        result = audit("DS-5-2019-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertIn(
            "legal_citations_not_treated_as_foreign",
            {signal["code"] for signal in result["signals"]},
        )

    def test_two_column_layout_does_not_imply_mixing(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO N° 005-2019-SA", 310, 30, 580, 65),
                block("Artículo 1.- Primera columna.", 20, 100, 285, 300),
                block("Artículo 2.- Segunda columna.", 315, 100, 580, 300),
            ],
        )
        result = audit("DS-5-2019-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertEqual(result["layout"]["detected_columns"], 2)

    def test_fragmented_table_cell_is_flagged_without_changing_scope(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[block("DECRETO SUPREMO N° 005-2019-SA", 30, 30, 570, 65)],
            tables=[
                table(
                    120,
                    260,
                    [
                        ["Funcionario", "Viáticos"],
                        ["Edward Allan Wagner Tizó", "n 370.00"],
                    ],
                )
            ],
        )
        result = audit("DS-5-2019-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertEqual(
            result["tables"][0]["fragmented_cells"][0]["reason"],
            "possible_trailing_letter_moved_to_next_cell",
        )
        self.assertIn(
            "La integridad de celdas es independiente de la pertenencia documental.",
            result["uncertainty"],
        )

    def test_adjacent_structural_continuity_can_verify_active_document(self):
        pages = [
            ScopePage(
                page_number=1,
                width=600,
                height=800,
                blocks=[block("DECRETO SUPREMO N° 005-2019-SA", 30, 30, 570, 65)],
            ),
            ScopePage(
                page_number=2,
                width=600,
                height=800,
                blocks=[
                    block("Artículo 3.- Continuación del articulado.", 30, 30, 570, 120)
                ],
            ),
        ]
        result = audit("DS-5-2019-SA", pages)["pages"][1]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertIn(
            "continuity_from_previous_page",
            {signal["code"] for signal in result["signals"]},
        )

    def test_non_adjacent_pages_do_not_inherit_document_continuity(self):
        pages = [
            ScopePage(
                page_number=1,
                width=600,
                height=800,
                blocks=[block("DECRETO SUPREMO N° 101-2024-SA", 30, 30, 570, 65)],
            ),
            ScopePage(
                page_number=8,
                width=600,
                height=800,
                blocks=[block("ANEXO N° 3", 30, 30, 570, 80)],
            ),
        ]

        result = audit("DS-101-2024-SA", pages)["pages"][1]

        self.assertEqual(result["classification"], SCOPE_UNKNOWN)
        self.assertFalse(result["document_flow"]["expected_active_at_start"])

    def test_citation_linker_prevents_false_new_disposition(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO", 75, 30, 225, 50),
                block("N° 101-2024-SA", 95, 52, 205, 72),
                block("ANEXO", 70, 300, 230, 320),
                block("Listado del reglamento aprobado por", 45, 325, 255, 350),
                block("Decreto Supremo N° 777-2005-SA", 60, 352, 240, 375),
            ],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertFalse(result["suspicious_regions"])
        citations = next(
            signal
            for signal in result["signals"]
            if signal["code"] == "legal_citations_not_treated_as_foreign"
        )
        self.assertIn("777-2005-SA", citations["evidence"])

    def test_general_same_sector_transition_is_still_mixed(self):
        page = ScopePage(
            page_number=2,
            width=600,
            height=800,
            blocks=[
                block("Artículo 4.- Final de la resolución activa.", 20, 30, 280, 130),
                block("Regístrese, comuníquese y publíquese.", 20, 140, 280, 165),
                block("RESOLUCIÓN MINISTERIAL", 330, 300, 560, 325),
                block("N° 889-2024/MINSA", 355, 328, 535, 350),
                block("CONSIDERANDO:", 330, 380, 560, 405),
            ],
        )

        result = audit_page_scope(
            page,
            parse_document_key("RM-888-2024-MINSA"),
            previous_expected_evidence=True,
        )

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertIn(
            "foreign_norm_heading", {signal["code"] for signal in result["signals"]}
        )

    def test_heading_split_inside_one_block_detects_new_disposition(self):
        page = ScopePage(
            page_number=2,
            width=600,
            height=800,
            blocks=[
                block("Artículo 4.- Final de la norma anterior.", 30, 30, 570, 100),
                block(
                    "RESOLUCIÓN MINISTERIAL\nN° 889-2024/MINSA",
                    80,
                    300,
                    520,
                    350,
                ),
            ],
        )

        result = audit_page_scope(
            page,
            parse_document_key("RM-888-2024-MINSA"),
            previous_expected_evidence=True,
        )

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        foreign = [
            signal
            for signal in result["signals"]
            if signal["code"] == "foreign_norm_heading"
        ]
        self.assertEqual(len(foreign), 1)
        self.assertIn("889-2024/MINSA", foreign[0]["evidence"])
        self.assertEqual(foreign[0]["bbox"]["y0"], 0.375)

    def test_split_legal_citation_inside_block_is_not_a_heading(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block(
                    "De conformidad con\nDECRETO SUPREMO\nN° 101-2024-SA,",
                    30,
                    100,
                    570,
                    180,
                )
            ],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_UNKNOWN)
        self.assertNotIn(
            "expected_norm_heading", {signal["code"] for signal in result["signals"]}
        )
        self.assertIn(
            "legal_citations_not_treated_as_foreign",
            {signal["code"] for signal in result["signals"]},
        )

    def test_split_heading_does_not_join_distant_or_cross_column_blocks(self):
        cases = {
            "distant": [
                block("DECRETO SUPREMO", 30, 30, 270, 50),
                block("N° 101-2024-SA", 30, 200, 270, 220),
            ],
            "cross_column": [
                block("DECRETO SUPREMO", 30, 30, 270, 50),
                block("N° 101-2024-SA", 330, 52, 570, 72),
            ],
        }

        for name, blocks in cases.items():
            with self.subTest(name=name):
                page = ScopePage(
                    page_number=1,
                    width=600,
                    height=800,
                    blocks=blocks,
                )
                result = audit("DS-101-2024-SA", [page])["pages"][0]

                self.assertEqual(result["classification"], SCOPE_UNKNOWN)
                self.assertNotIn(
                    "expected_norm_heading",
                    {signal["code"] for signal in result["signals"]},
                )

    def test_table_uses_latest_active_disposition(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO N° 100-2024-SA", 30, 30, 570, 65),
                block("Contenido de DS-100", 30, 80, 570, 150),
                block("DECRETO SUPREMO N° 101-2024-SA", 30, 300, 570, 335),
                block("Contenido de DS-101", 30, 350, 570, 420),
            ],
            tables=[
                table(180, 260, [["Tabla", "DS-100"]]),
                table(450, 600, [["Tabla", "DS-101"]]),
            ],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertTrue(result["tables"][0]["suspicious_scope"])
        self.assertIn(
            "table_belongs_to_active_foreign_disposition",
            result["tables"][0]["reasons"],
        )
        self.assertFalse(result["tables"][1]["suspicious_scope"])

    def test_two_column_table_follows_heading_in_its_reading_flow(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO N° 100-2024-SA", 20, 30, 280, 65),
                block("Contenido anterior", 20, 80, 280, 180),
                block("DECRETO SUPREMO N° 101-2024-SA", 320, 30, 580, 65),
                block("Contenido objetivo", 320, 80, 580, 180),
            ],
            tables=[
                table(220, 350, [["Tabla", "objetivo"]], x0=320, x1=580),
            ],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["layout"]["detected_columns"], 2)
        self.assertFalse(result["tables"][0]["suspicious_scope"])

    def test_table_crossing_columns_remains_pending_when_order_is_ambiguous(self):
        page = ScopePage(
            page_number=1,
            width=600,
            height=800,
            blocks=[
                block("DECRETO SUPREMO N° 101-2024-SA", 20, 30, 280, 65),
                block("Artículo 1.- Columna izquierda.", 20, 80, 280, 180),
                block("Artículo 2.- Columna derecha.", 320, 80, 580, 180),
            ],
            tables=[table(220, 400, [["Tabla", "ancha"]], x0=20, x1=580)],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertIn(
            "table_reading_order_ambiguous_across_columns",
            result["tables"][0]["reasons"],
        )

    def test_annex_citation_does_not_verify_page_scope(self):
        page = ScopePage(
            page_number=4,
            width=600,
            height=800,
            blocks=[
                block(
                    "De conformidad con el Anexo del Decreto Supremo\n"
                    "N° 101-2024-SA, se establecen los siguientes requisitos.",
                    30,
                    100,
                    570,
                    200,
                )
            ],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_UNKNOWN)
        self.assertNotIn(
            "annex_explicitly_bound_to_expected_norm",
            {signal["code"] for signal in result["signals"]},
        )

    def test_true_annex_heading_still_verifies_expected_scope(self):
        page = ScopePage(
            page_number=4,
            width=600,
            height=800,
            blocks=[
                block(
                    "ANEXO N° 2 DEL DECRETO SUPREMO N° 101-2024-SA\nRequisitos",
                    30,
                    30,
                    570,
                    100,
                )
            ],
        )

        result = audit("DS-101-2024-SA", [page])["pages"][0]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertIn(
            "annex_explicitly_bound_to_expected_norm",
            {signal["code"] for signal in result["signals"]},
        )

    def test_minister_signatures_do_not_change_document_scope(self):
        pages = [
            ScopePage(
                page_number=1,
                width=600,
                height=800,
                blocks=[
                    block("DECRETO SUPREMO N° 101-2024-SA", 30, 30, 570, 65),
                    block("Artículo 1.- Objeto.", 30, 100, 570, 180),
                ],
            ),
            ScopePage(
                page_number=2,
                width=600,
                height=800,
                blocks=[
                    block("Artículo 2.- Continuación.", 30, 30, 570, 100),
                    block(
                        "MINISTRO DE JUSTICIA Y DERECHOS HUMANOS", 320, 150, 570, 180
                    ),
                    block("MINISTRA DE ECONOMÍA Y FINANZAS", 320, 190, 570, 220),
                    block(
                        "4.2 El Ministerio de Agricultura y Riego, a través de:",
                        30,
                        250,
                        570,
                        290,
                    ),
                ],
            ),
        ]

        result = audit("DS-101-2024-SA", pages)["pages"][1]

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertNotIn(
            "foreign_ministry_heading",
            {signal["code"] for signal in result["signals"]},
        )


class RealPdfLayoutRegressionTest(unittest.TestCase):
    """Fixtures transcritos de los bloques/coords de los dos PDF oficiales."""

    width = 488.97601318359375
    height = 779.5280151367188

    def test_real_ds_page_1_detects_transition_and_three_foreign_tables(self):
        page = ScopePage(
            page_number=1,
            printed_page="35",
            width=self.width,
            height=self.height,
            blocks=[
                block(
                    "Reg�strese, comun�quese y publ�quese.", 48.2, 296.2, 189.1, 304.2
                ),
                block(
                    "N�STOR POPOLIZIO BARDALES\nMinistro de Relaciones Exteriores",
                    48.2,
                    313.0,
                    171.6,
                    329.4,
                ),
                block("RESOLUCI�N MINISTERIAL", 84.2, 399.6, 190.9, 407.6),
                block("N� 0120/RE-2019", 106.2, 407.6, 168.7, 415.6),
                block(
                    "Reg�strese, comun�quese y publ�quese.", 269.3, 373.1, 410.3, 381.1
                ),
                block(
                    "N�STOR POPOLIZIO BARDALES\nMinistro de Relaciones Exteriores",
                    269.3,
                    389.9,
                    392.7,
                    406.3,
                ),
                block("SALUD", 340.2, 441.2, 377.0, 453.2),
                block("DECRETO SUPREMO", 317.8, 516.4, 399.4, 524.4),
                block("N� 005-2019-SA", 329.5, 524.4, 388.0, 532.4),
                block("CONSIDERANDO:", 269.3, 559.7, 336.9, 567.7),
            ],
            tables=[
                ScopeTable(
                    bbox=(34.4, 124.3, 238.0, 156.4),
                    rows=[["Edward Allan Wagner Tizó", "n 370.00"]],
                ),
                ScopeTable(bbox=(34.4, 165.0, 238.0, 213.8), rows=[["A", "100"]]),
                ScopeTable(bbox=(256.7, 233.5, 459.1, 290.8), rows=[["B", "200"]]),
            ],
        )

        result = audit("DS-5-2019", [page])["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertTrue(result["has_expected_identity_evidence"])
        self.assertEqual(
            len(
                [
                    region
                    for region in result["suspicious_regions"]
                    if region["kind"] == "table"
                ]
            ),
            3,
        )
        self.assertTrue(all(item["suspicious_scope"] for item in result["tables"]))

    def test_real_ds_page_12_keeps_approved_regulation_as_citation(self):
        page = ScopePage(
            page_number=12,
            printed_page="46",
            width=self.width,
            height=self.height,
            blocks=[
                block("ANEXO N� 02", 105.1, 80.3, 155.6, 88.3),
                block(
                    "CAUSALES DE SUSPENSI�N O CANCELACI�N DE LICENCIAS",
                    67.3,
                    97.1,
                    193.5,
                    113.5,
                ),
                block("ANEXO", 116.2, 613.9, 144.6, 621.9),
                block(
                    "Modificaci�n de las Listas I A, I B, IIA y IVA del Anexo N� 2 del Reglamento de Estupefacientes,",
                    33.1,
                    628.9,
                    230.0,
                    648.4,
                ),
                block(
                    "Psicotr�picos y Otras Sustancias Sujetas",
                    51.5,
                    645.7,
                    209.3,
                    656.8,
                ),
                block(
                    "a Fiscalizaci�n Sanitaria, aprobado por", 56.2, 654.1, 204.6, 665.2
                ),
                block("Decreto Supremo N� 023-2001-SA", 66.8, 662.5, 194.2, 673.6),
                block("�ANEXO N� 2", 105.4, 693.1, 155.4, 701.1),
            ],
            tables=[
                ScopeTable(bbox=(28.5, 124.3, 231.0, 598.8), rows=[["1", "Causal"]])
            ],
        )

        result = audit_page_scope(
            page,
            parse_document_key("DS-5-2019"),
            previous_expected_evidence=True,
        )

        self.assertEqual(result["classification"], SCOPE_VERIFIED)
        self.assertFalse(result["suspicious_regions"])
        self.assertIn(
            "legal_citations_not_treated_as_foreign",
            {signal["code"] for signal in result["signals"]},
        )

    def test_real_ds_page_13_detects_same_sector_transition(self):
        page = ScopePage(
            page_number=13,
            printed_page="47",
            width=self.width,
            height=self.height,
            blocks=[
                block("LISTA IV A\n1. Acetildihidrocodeína", 35.4, 399.5, 241.8, 508.4),
                block(
                    "Designan Jefa de la Unidad de Logística",
                    256.5,
                    308.6,
                    462.6,
                    342.1,
                ),
                block("RESOLUCI�N MINISTERIAL", 305.3, 350.2, 412.0, 358.2),
                block("N� 174-2019/MINSA", 322.2, 358.2, 395.2, 366.2),
                block("CONSIDERANDO:", 269.3, 433.9, 336.8, 441.9),
            ],
        )

        result = audit_page_scope(
            page,
            parse_document_key("DS-5-2019"),
            previous_expected_evidence=True,
        )

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        foreign = [
            signal
            for signal in result["signals"]
            if signal["code"] == "foreign_norm_heading"
        ]
        self.assertEqual(len(foreign), 1)
        self.assertIn("174-2019/MINSA", foreign[0]["evidence"])

    def test_real_rm_page_1_keeps_target_table_and_flags_prior_disposition(self):
        page = ScopePage(
            page_number=1,
            printed_page="21",
            width=self.width,
            height=self.height,
            blocks=[
                block(
                    "Reg�strese, comun�quese y publ�quese.", 48.2, 114.0, 189.1, 122.0
                ),
                block(
                    "N�STOR POPOLIZIO BARDALES\nMinistro de Relaciones Exteriores",
                    48.2,
                    130.8,
                    171.6,
                    147.2,
                ),
                block("SALUD", 119.1, 181.6, 155.9, 193.9),
                block("RESOLUCI�N MINISTERIAL", 84.2, 232.7, 190.9, 240.7),
                block("N� 250-2019/MINSA", 101.1, 240.7, 174.1, 248.7),
                block("CONSIDERANDO:", 48.2, 376.8, 115.7, 384.8),
                block(
                    "Que, la Primera Disposici�n Complementaria Final del Decreto Supremo N� 075-2017-PCM, precisa reglas aplicables.",
                    35.4,
                    712.4,
                    241.8,
                    762.4,
                ),
            ],
            tables=[
                ScopeTable(bbox=(256.7, 569.3, 458.8, 626.5), rows=[["TOTAL", "47"]])
            ],
        )

        result = audit("RM-250-2019", [page])["pages"][0]

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        self.assertFalse(result["tables"][0]["suspicious_scope"])
        citations = next(
            signal
            for signal in result["signals"]
            if signal["code"] == "legal_citations_not_treated_as_foreign"
        )
        self.assertIn("075-2017-PCM", citations["evidence"])

    def test_real_rm_page_2_detects_two_new_dispositions(self):
        page = ScopePage(
            page_number=2,
            printed_page="22",
            width=self.width,
            height=self.height,
            blocks=[
                block(
                    "Artículo 5.- La presente Resolución Ministerial y sus Anexos se publican.",
                    28.4,
                    372.7,
                    234.7,
                    442.0,
                ),
                block(
                    "Reg�strese, comun�quese y publ�quese.", 41.1, 450.0, 182.1, 458.0
                ),
                block("RESOLUCI�N MINISTERIAL", 77.1, 551.8, 183.8, 559.8),
                block("N� 260-2019/MINSA", 94.2, 559.8, 166.9, 567.8),
                block("RESOLUCI�N MINISTERIAL", 298.2, 331.7, 404.9, 339.7),
                block("N� 261-2019/MINSA", 315.3, 339.7, 388.0, 347.7),
            ],
        )

        result = audit_page_scope(
            page,
            parse_document_key("RM-250-2019"),
            previous_expected_evidence=True,
        )

        self.assertEqual(result["classification"], MIXED_PENDING_REVIEW)
        foreign = [
            signal
            for signal in result["signals"]
            if signal["code"] == "foreign_norm_heading"
        ]
        self.assertEqual(len(foreign), 2)
        self.assertFalse(result["document_flow"]["expected_active_at_end"])


if __name__ == "__main__":
    unittest.main()
