import unittest

from agents import ocr_preproceso


@unittest.skipUnless(ocr_preproceso.HAS_CV2, "OpenCV no instalado")
class AnguloConRuidoTest(unittest.TestCase):
    def test_detecta_inclinacion_aun_con_ruido_y_enderezar_la_corrige(self):
        import cv2
        import numpy as np

        pagina = np.full((1100, 800), 255, np.uint8)
        for fila in range(100, 1000, 45):
            cv2.putText(pagina, "TEXTO DE PRUEBA DIGEMID BOTICA", (60, fila), cv2.FONT_HERSHEY_SIMPLEX, 0.9, 0, 2)
        m = cv2.getRotationMatrix2D((400, 550), 4.0, 1.0)
        inclinada = cv2.warpAffine(pagina, m, (800, 1100), borderValue=255)
        rng = np.random.default_rng(1)
        sucia = np.clip(inclinada + rng.normal(0, 20, inclinada.shape), 0, 255).astype(np.uint8)

        angulo = ocr_preproceso.estimar_angulo(sucia)
        self.assertAlmostEqual(abs(angulo), 4.0, delta=0.6)

        recta = ocr_preproceso.enderezar(sucia, angulo)
        self.assertLess(abs(ocr_preproceso.estimar_angulo(recta)), 0.7)


if __name__ == "__main__":
    unittest.main()
