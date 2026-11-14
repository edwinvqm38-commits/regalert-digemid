"""Las alertas de DIGEMID son gratis para todo usuario registrado, y la prueba
de IA (2 consultas/dia por 5 dias) no corta ni cuenta las alertas."""

import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "token-de-prueba")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-100")

from agents import agent_notify
from agents.agent_notify import NotifyAgent


class _Consulta:
    def __init__(self, filas):
        self.filas = filas
        self.rango = None

    def select(self, *_):
        return self

    def range(self, desde, hasta):
        self.rango = (desde, hasta)
        return self

    def execute(self):
        desde, hasta = self.rango
        return MagicMock(data=self.filas[desde : hasta + 1])


class _SupabaseFalso:
    def __init__(self, filas):
        self.filas = filas
        self.escrituras = []

    def table(self, nombre):
        assert nombre == "digemid_bot_usuarios"
        return _Consulta(self.filas)


def _agente(filas):
    agente = NotifyAgent()
    agente.supabase = _SupabaseFalso(filas)
    return agente


def test_alertas_llegan_a_todos_los_registrados_sin_importar_la_prueba():
    filas = [{"telegram_chat_id": str(i)} for i in range(1, 6)]
    agente = _agente(filas)
    enviados = []

    with patch.object(agente, "_send_to_chat", side_effect=lambda c, t, *a: enviados.append(c)):
        with patch.object(agente, "build_message", return_value="alerta"):
            agente.send_individual_alerts([{"document_key": "ALERTA-1"}])

    assert sorted(enviados) == ["1", "2", "3", "4", "5"]


def test_la_lista_de_destinatarios_pagina_mas_de_mil_usuarios():
    filas = [{"telegram_chat_id": str(i)} for i in range(2500)]
    agente = _agente(filas)

    assert len(agente._usuarios_alertas_gratis()) == 2500


def test_enviar_alertas_no_modifica_la_prueba_de_nadie():
    agente = _agente([{"telegram_chat_id": "1"}])

    with patch.object(agente, "_send_to_chat"), patch.object(agente, "build_message", return_value="a"):
        agente.send_individual_alerts([{"document_key": "ALERTA-1"}])

    assert agente.supabase.escrituras == []


def test_la_prueba_de_ia_dura_cinco_dias():
    from scripts import finalizar_pruebas_vencidas

    assert agent_notify.PRUEBA_LIMITE_DIAS == 5
    assert finalizar_pruebas_vencidas.PRUEBA_LIMITE_DIAS == 5
