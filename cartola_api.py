# %%
import asyncio
import os
import time
from datetime import datetime

import httpx
import nest_asyncio
import numpy as np
import pandas as pd
from httpx import Limits
from pandas import json_normalize

nest_asyncio.apply()


class api(object):
    def __init__(self, _useragent, _globoid, _token, _times, _diretorio):
        os.chdir(_diretorio)

        self.cwd = os.getcwd()
        self.useragent = _useragent
        self.globoid = _globoid
        self.token = _token
        self.times = _times
        self._limits = Limits(
            max_connections=10, max_keepalive_connections=5, keepalive_expiry=5.0
        )
        self.headers = {"User-Agent": self.useragent, "X-GLB-Token": self.token}

        self.criar_diretorio("./DADOS/ARQLOG")
        self.criar_diretorio("./DADOS/STATUSMERCADO")
        self.criar_diretorio("./DADOS/MERCADO")
        self.criar_diretorio("./DADOS/POSICOES")
        self.criar_diretorio("./DADOS/STATUS")
        self.criar_diretorio("./DADOS/CLUBES")
        self.criar_diretorio("./DADOS/LIGA")
        self.criar_diretorio("./DADOS/PARTIDASRODADA")
        self.criar_diretorio("./DADOS/PONTUACAORODADA")
        self.criar_diretorio("./DADOS/ESCALACAO")
        self.criar_diretorio("./DADOS/CAPITAO")
        self.criar_diretorio("./DADOS/TITULAR")
        self.criar_diretorio("./DADOS/RESERVA")
        self.criar_diretorio("./DADOS/RESERVALUXO")

        self.mercado_status()
        self.mercado()
        self.posicoes()
        self.status()
        self.clubes()
        self.partidas()
        self.liga()
        self.escalacao()
        self.pontuacao()
        self.log()

    def tic(self):
        self.start_time = time.time()

    def tac(self):
        self.end_time = time.time()

        t_sec = round(self.end_time - self.start_time)
        (t_min, t_sec) = divmod(t_sec, 60)
        (t_hour, t_min) = divmod(t_min, 60)
        t_nsec = (self.end_time - self.start_time) - t_sec
        t_nsec = ("." + str(t_nsec).split(".")[1])[0:4]
        print(
            "Tempo de execução: {:0>2}h:{:0>2}m:{:0>2}s{}\n".format(
                t_hour, t_min, t_sec, t_nsec
            )
        )

    def log(self):
        tempo = datetime.now()
        texto = "Extração;{}\n".format(tempo)
        with open(
            "{}/DADOS/ARQLOG/arqlog.csv".format(self.cwd), "a", encoding="utf-8"
        ) as arqlog:
            arqlog.write(texto)

    def criar_diretorio(self, caminho):
        try:
            os.makedirs(caminho)
            print(f"Diretório '{caminho}' criado com sucesso.")
        except FileExistsError:
            print(f"O diretório '{caminho}' já existe.")
        except OSError as error:
            print(f"Erro ao criar o diretório: {error}")

    def mercado_status(self):
        auth_url = "https://api.cartolafc.globo.com/mercado/status"

        with httpx.Client(headers=self.headers) as client:
            response = client.get(auth_url, follow_redirects=True)

        body = response.json()
        rodada_atual = body["rodada_atual"]
        status_mercado = body["status_mercado"]

        name_status_mercado = "Aberto" if status_mercado == 1 else "Fechado"
        df_statusmercado = pd.DataFrame(
            {"rodada_atual": [rodada_atual], "status_mercado": [name_status_mercado]}
        )
        df_statusmercado.to_csv(
            "{}/DADOS/STATUSMERCADO/statusmercado.csv".format(self.cwd),
            index=False,
            header=True,
            sep=";",
            encoding="utf-8",
        )

        return status_mercado, rodada_atual

    def mercado(self):
        self.tic()
        global df_mercado

        if self.mercado_status()[0] == 1:
            print(
                "\nMercado aberto, estamos na rodada {}".format(
                    self.mercado_status()[1]
                )
            )
            print("Obtendo informações do Mercado")

        else:
            print(
                "\nMercado Fechado, rodada {} em andamento.".format(
                    self.mercado_status()[1]
                )
            )
            print("Obtendo informações do Mercado")

        auth_url = "https://api.cartolafc.globo.com/atletas/mercado"

        with httpx.Client(headers=self.headers) as client:
            response = client.get(auth_url, follow_redirects=True)

        body = response.json()["atletas"]
        dado = json_normalize(data=body, sep="")

        df_mercado = pd.DataFrame()
        df_mercado = pd.concat([df_mercado, dado], ignore_index=True)
        df_mercado["rodada_id"] = self.mercado_status()[1]

        df_mercado = df_mercado[
            [
                "atleta_id",
                "rodada_id",
                "clube_id",
                "posicao_id",
                "status_id",
                "pontos_num",
                "preco_num",
                "variacao_num",
                "media_num",
                "jogos_num",
                "minimo_para_valorizar",
                "slug",
                "apelido",
                "apelido_abreviado",
                "nome",
                "foto",
            ]
        ]

        df_mercado["slug"] = df_mercado["slug"].str.strip()
        df_mercado["apelido"] = df_mercado["apelido"].str.strip()
        df_mercado["apelido_abreviado"] = df_mercado["apelido_abreviado"].str.strip()
        df_mercado["nome"] = df_mercado["nome"].str.strip()

        df_mercado["minimo_para_valorizar"] = np.where(
            df_mercado["minimo_para_valorizar"].isna(),
            0,
            df_mercado["minimo_para_valorizar"],
        )

        df_mercado = df_mercado.rename(
            columns={
                "slug": "atleta_slug",
                "apelido": "atleta_apelido",
                "apelido_abreviado": "atleta_apelido_abreviado",
                "nome": "atleta_nome",
                "foto": "atleta_foto",
            }
        )

        df_mercado.to_csv(
            "{}/DADOS/MERCADO/mercado_{:0>2}.csv".format(
                self.cwd, self.mercado_status()[1]
            ),
            index=False,
            header=True,
            sep=";",
            encoding="utf-8",
        )

        self.tac()

    def posicoes(self):
        self.tic()
        print("Obtendo Posições")

        auth_url = "https://api.cartolafc.globo.com/atletas/mercado"
        with httpx.Client(headers=self.headers) as client:
            response = client.get(auth_url, follow_redirects=True)

        body = response.json()["posicoes"]

        df_posicoes = pd.DataFrame.from_dict(body)
        df_posicoes = df_posicoes.T.reset_index(drop=True)

        df_posicoes = df_posicoes.rename(
            columns={
                "id": "posicao_id",
                "nome": "posicao_nome",
                "abreviacao": "posicao_abreviacao",
            }
        )

        df_posicoes.to_csv(
            "{}/DADOS/POSICOES/posicoes.csv".format(self.cwd),
            index=False,
            header=True,
            sep=";",
            encoding="utf-8",
        )

        self.tac()

    def status(self):
        self.tic()
        print("Obtendo Status")
        auth_url = "https://api.cartolafc.globo.com/atletas/mercado"

        with httpx.Client(headers=self.headers) as client:
            response = client.get(auth_url, follow_redirects=True)

        body = response.json()["status"]

        df_status = pd.DataFrame.from_dict(body)
        df_status = df_status.T.reset_index(drop=True)

        df_status = df_status.rename(columns={"nome": "status_nome", "id": "status_id"})

        df_status.to_csv(
            "{}/DADOS/STATUS/status.csv".format(self.cwd),
            index=False,
            header=True,
            sep=";",
            encoding="utf-8",
        )

        self.tac()

    def clubes(self):
        self.tic()
        print("Obtendo Times")
        auth_url = "https://api.cartolafc.globo.com/clubes"
        with httpx.Client(headers=self.headers) as client:
            response = client.get(auth_url, follow_redirects=True)

        body = response.json()

        df_clubes = pd.DataFrame.from_dict(body)
        df_clubes = df_clubes.T.reset_index(drop=True)
        df_escudos = pd.DataFrame(df_clubes["escudos"].values.tolist())
        df_clubes = pd.concat([df_clubes, df_escudos], axis=1)
        df_clubes = df_clubes.drop(["escudos"], axis=1)
        df_clubes = df_clubes.rename(
            columns={
                "id": "clubeid",
                "nome": "clube_nome",
                "abreviacao": "clube_abreviacao",
                "nome_fantasia": "clube_nome_fantasia",
                "60x60": "_60x60",
                "45x45": "_45x45",
                "30x30": "_30x30",
            }
        )
        df_clubes.to_csv(
            "{}/DADOS/CLUBES/clubes.csv".format(self.cwd),
            index=False,
            header=True,
            sep=";",
            encoding="utf-8",
        )
        self.tac()

    def liga(self):
        self.tic()
        print("Obtendo informações da Liga:")

        self.df_liga = pd.DataFrame()

        async def get_liga(client, auth_url, time):
            print("- {}".format(time))
            response = await client.get(auth_url, follow_redirects=True)
            body = response.json()
            dados = json_normalize(data=body, sep="")

            self.df_liga = pd.concat([self.df_liga, dados], ignore_index=True)

        async def main():
            async with httpx.AsyncClient(timeout=None) as client:
                tasks = []
                for time in self.times:
                    auth_url = "https://api.cartolafc.globo.com/times?q={}".format(time)
                    tasks.append(
                        asyncio.ensure_future(get_liga(client, auth_url, time))
                    )

                await asyncio.gather(*tasks)

        asyncio.run(main())

        self.df_liga = self.df_liga.rename(
            columns={
                "nome_cartola": "cartola_nome",
                "slug": "cartola_slug",
                "nome": "cartola_time",
            }
        )

        self.df_liga.to_csv(
            "{}/DADOS/LIGA/liga.csv".format(self.cwd),
            index=False,
            header=True,
            sep=";",
            encoding="utf-8",
        )
        self.tac()

    def partidas(self):
        self.tic()
        print("Obtendo partidas das rodadas")

        if self.mercado_status()[0] == 1:
            looprodadas = range(1, 39)
        else:
            looprodadas = range(self.mercado_status()[1], self.mercado_status()[1] + 1)

        async def get_partidas(client, auth_url, rodada):
            response = await client.get(auth_url, follow_redirects=True)
            body = response.json()
            dados = json_normalize(data=body, sep="")

            df_partida_rodada = pd.DataFrame()
            df_partida_rodada = pd.concat([df_partida_rodada, dados], ignore_index=True)

            df_partida_rodada = pd.DataFrame(
                df_partida_rodada["partidas"].values.tolist()
            ).T
            df_partida_rodada = pd.json_normalize(df_partida_rodada[0])
            df_partida_rodada["rodada"] = rodada

            df_apm = pd.DataFrame(
                df_partida_rodada["aproveitamento_mandante"].values.tolist()
            )
            df_apm.rename(
                columns={0: "apm0", 1: "apm1", 2: "apm2", 3: "apm3", 4: "apm4"},
                inplace=True,
            )
            df_apv = pd.DataFrame(
                df_partida_rodada["aproveitamento_visitante"].values.tolist()
            )
            df_apv.rename(
                columns={0: "apv0", 1: "apv1", 2: "apv2", 3: "apv3", 4: "apv4"},
                inplace=True,
            )

            df_partida_rodada = pd.concat([df_partida_rodada, df_apm], axis=1)
            df_partida_rodada = pd.concat([df_partida_rodada, df_apv], axis=1)
            df_partida_rodada = df_partida_rodada.drop(
                ["aproveitamento_mandante", "aproveitamento_visitante"], axis=1
            )

            df_partida_rodada["periodo_tr"] = df_partida_rodada["periodo_tr"].replace(
                "", "-"
            )

            df_partida_rodada["rodada"] = rodada
            df_partida_rodada["rodada_id"] = rodada
            df_partida_rodada["rodada_atual"] = self.mercado_status()[1]

            columns_rename = {
                "transmissao.label": "transmissaolabel",
                "transmissao.url": "transmissaourl",
            }
            df_partida_rodada.rename(columns=columns_rename, inplace=True)

            df_partida_rodada["placar_oficial_mandante"] = df_partida_rodada[
                "placar_oficial_mandante"
            ].fillna(0)
            df_partida_rodada["placar_oficial_visitante"] = df_partida_rodada[
                "placar_oficial_visitante"
            ].fillna(0)

            df_partida_rodada = df_partida_rodada.astype(
                {
                    "placar_oficial_mandante": "Int64",
                    "placar_oficial_visitante": "Int64",
                }
            )

            df_partida_rodada.to_csv(
                "{}/DADOS/PARTIDASRODADA/partidasrodada_{:0>2}.csv".format(
                    self.cwd, rodada
                ),
                index=False,
                header=True,
                sep=";",
                encoding="utf-8",
            )

        async def main():
            async with httpx.AsyncClient(timeout=None) as client:
                tasks = []
                for rodada in looprodadas:
                    auth_url = "https://api.cartolafc.globo.com/partidas/{}".format(
                        rodada
                    )
                    tasks.append(
                        asyncio.ensure_future(get_partidas(client, auth_url, rodada))
                    )

                await asyncio.gather(*tasks)

        asyncio.run(main())
        self.tac()

    def pontuacao(self):
        self.tic()
        print("Obtendo a pontuaçao das rodadas")
        status = self.mercado_status()[0]
        rodada = self.mercado_status()[1]

        global df_pontuacao
        df_pontuacao = pd.DataFrame()

        if status == 1:
            status_rodada01 = 1
            status_rodada02 = rodada
        else:
            status_rodada01 = rodada
            status_rodada02 = rodada + 1

        looprodadas = range(status_rodada01, status_rodada02)

        async def get_pontuacao(client, auth_url, rodd):
            # print(rodd)

            response = await client.get(auth_url, follow_redirects=True)
            body = response.json()["atletas"]

            df_pontuacaorodadafinal = pd.DataFrame(
                columns=[
                    "rodada",
                    "atleta_id",
                    "scout",
                    "pontuacao",
                    "posicao_id",
                    "clube_id",
                    "entrou_em_campo",
                    "G",
                    "A",
                    "FT",
                    "FD",
                    "FF",
                    "FS",
                    "PS",
                    "PP",
                    "I",
                    "PI",
                    "DP",
                    "SG",
                    "DE",
                    "DS",
                    "GC",
                    "CV",
                    "CA",
                    "GS",
                    "FC",
                    "PC",
                    "V",
                ]
            )

            df_pontuacaorodada = pd.DataFrame.from_dict(body)
            df_pontuacaorodada = df_pontuacaorodada.T.reset_index()
            name_columns = {
                "index": "atleta_id",
                0: "scout",
                1: "apelido",
                2: "foto",
                3: "pontuacao",
                4: "posicao_id",
                5: "clube_id",
                6: "entrou_em_campo",
            }
            df_pontuacaorodada.rename(
                columns=name_columns,
                inplace=True,
            )
            df_pontuacaorodada["rodada"] = rodd
            df_pontuacaorodada = df_pontuacaorodada[
                [
                    "rodada",
                    "atleta_id",
                    "scout",
                    "pontuacao",
                    "posicao_id",
                    "clube_id",
                    "entrou_em_campo",
                ]
            ]

            df_scout = pd.DataFrame()

            for _scout in df_pontuacaorodada["scout"]:
                if _scout == None:
                    _scout = {}
                else:
                    _scout = _scout

                df_scout_linha = pd.DataFrame.from_dict(_scout, orient="index").T
                df_scout = pd.concat([df_scout, df_scout_linha])

            df_pont = pd.DataFrame(
                columns=[
                    "G",
                    "A",
                    "FT",
                    "FD",
                    "FF",
                    "FS",
                    "PS",
                    "PP",
                    "I",
                    "PI",
                    "DP",
                    "SG",
                    "DE",
                    "DS",
                    "GC",
                    "CV",
                    "CA",
                    "GS",
                    "FC",
                    "PC",
                    "V",
                ]
            )

            df_pontuacaorodada = pd.concat(
                [
                    df_pontuacaorodada,
                    df_pontuacaorodada["scout"].apply(
                        lambda x: pd.Series(x, dtype="str")
                    ),
                ],
                axis=1,
            )

            df_pontuacaorodadafinal = pd.concat(
                [df_pontuacaorodadafinal, df_pontuacaorodada], ignore_index=True
            )

            df_pontuacaorodadafinal = df_pontuacaorodadafinal.drop(columns="scout")
            df_pontuacaorodadafinal = df_pontuacaorodadafinal.sort_values(
                ["rodada", "pontuacao"], ascending=([True, False])
            ).reset_index(drop=True)

            df_pontuacaorodadafinal = df_pontuacaorodadafinal.rename(
                columns={"rodada": "rodada_id"}
            )

            df_pontuacaorodadafinal["G"] = df_pontuacaorodadafinal["G"].fillna("0")
            df_pontuacaorodadafinal["A"] = df_pontuacaorodadafinal["A"].fillna("0")
            df_pontuacaorodadafinal["FT"] = df_pontuacaorodadafinal["FT"].fillna("0")
            df_pontuacaorodadafinal["FD"] = df_pontuacaorodadafinal["FD"].fillna("0")
            df_pontuacaorodadafinal["FF"] = df_pontuacaorodadafinal["FF"].fillna("0")
            df_pontuacaorodadafinal["FS"] = df_pontuacaorodadafinal["FS"].fillna("0")
            df_pontuacaorodadafinal["PS"] = df_pontuacaorodadafinal["PS"].fillna("0")
            df_pontuacaorodadafinal["PP"] = df_pontuacaorodadafinal["PP"].fillna("0")
            df_pontuacaorodadafinal["I"] = df_pontuacaorodadafinal["I"].fillna("0")
            df_pontuacaorodadafinal["PI"] = df_pontuacaorodadafinal["PI"].fillna("0")
            df_pontuacaorodadafinal["DP"] = df_pontuacaorodadafinal["DP"].fillna("0")
            df_pontuacaorodadafinal["SG"] = df_pontuacaorodadafinal["SG"].fillna("0")
            df_pontuacaorodadafinal["DE"] = df_pontuacaorodadafinal["DE"].fillna("0")
            df_pontuacaorodadafinal["DS"] = df_pontuacaorodadafinal["DS"].fillna("0")
            df_pontuacaorodadafinal["GC"] = df_pontuacaorodadafinal["GC"].fillna("0")
            df_pontuacaorodadafinal["CV"] = df_pontuacaorodadafinal["CV"].fillna("0")
            df_pontuacaorodadafinal["CA"] = df_pontuacaorodadafinal["CA"].fillna("0")
            df_pontuacaorodadafinal["GS"] = df_pontuacaorodadafinal["GS"].fillna("0")
            df_pontuacaorodadafinal["FC"] = df_pontuacaorodadafinal["FC"].fillna("0")
            df_pontuacaorodadafinal["PC"] = df_pontuacaorodadafinal["PC"].fillna("0")
            df_pontuacaorodadafinal["V"] = df_pontuacaorodadafinal["V"].fillna("0")

            df_pontuacaorodadafinal = df_pontuacaorodadafinal.astype(
                {
                    "G": int,
                    "A": int,
                    "FT": int,
                    "FD": int,
                    "FF": int,
                    "FS": int,
                    "PS": int,
                    "PP": int,
                    "I": int,
                    "PI": int,
                    "DP": int,
                    "SG": int,
                    "DE": int,
                    "DS": int,
                    "GC": int,
                    "CV": int,
                    "CA": int,
                    "GS": int,
                    "FC": int,
                    "PC": int,
                    "V": int,
                }
            )

            df_pontuacaorodadafinal = df_pontuacaorodadafinal.astype(
                {
                    "pontuacao": float,
                }
            )

            df_pontuacaorodadafinal.to_csv(
                "{}/DADOS/PONTUACAORODADA/pontuacaorodada_{:0>2}.csv".format(
                    self.cwd, rodd
                ),
                index=False,
                header=True,
                sep=";",
                encoding="utf-8",
            )

        async def main():
            async with httpx.AsyncClient(limits=self._limits, timeout=None) as client:
                tasks = []
                for rodd in looprodadas:
                    rod = "" if rodd == rodada else rodd

                    auth_url = (
                        "https://api.cartolafc.globo.com/atletas/pontuados/{}".format(
                            rod
                        )
                    )
                    tasks.append(
                        asyncio.ensure_future(get_pontuacao(client, auth_url, rodd))
                    )

                await asyncio.gather(*tasks)

        asyncio.run(main())
        self.tac()

    def escalacao(self):
        self.tic()
        print("Obtendo escalação")
        rodadas = self.mercado_status()[1]
        lista_liga = self.df_liga["time_id"]

        async def get_escalacao(client, auth_url, time, rodada):
            df_escalacao = pd.DataFrame()
            df_capitao = pd.DataFrame()
            df_titular = pd.DataFrame()
            df_reserva = pd.DataFrame()
            df_reservaluxo = pd.DataFrame()

            response = await client.get(auth_url, follow_redirects=True)
            body = response.json()
            dados = json_normalize(data=body, sep="")

            # ESCALACAO
            df_e = pd.DataFrame()
            df_e = pd.concat([df_e, dados], ignore_index=True)

            df_e["rodada"] = rodada
            df_e["rodada_id"] = rodada
            df_e["atletas"] = df_e["atletas"].astype(str) + "]"
            df_e["atletas"] = df_e["atletas"].values.tolist()
            df_e = df_e[
                [
                    "rodada",
                    "timetime_id",
                    "timerodada_time_id",
                    "rodada_atual",
                    "patrimonio",
                    "valor_time",
                    "pontos",
                    "pontos_campeonato",
                ]
            ]
            df_e = df_e.drop_duplicates(subset=["rodada", "timetime_id"])

            df_escalacao = pd.concat([df_escalacao, df_e], ignore_index=True)
            df_escalacao = df_escalacao.drop_duplicates(
                subset=["rodada", "timetime_id"]
            )
            df_escalacao = df_escalacao.astype({"timetime_id": int})
            df_escalacao = df_escalacao.sort_values(
                ["rodada", "pontos"], ascending=([True, True])
            ).reset_index(drop=True)

            df_escalacao = df_escalacao.rename(
                columns={
                    "rodada": "rodada_id",
                    "timetime_id": "time_id",
                    "timerodada_time_id": "time_rodada",
                }
            )

            df_escalacao.to_csv(
                "{}/DADOS/ESCALACAO/escalacao_{}_{:0>2}.csv".format(
                    self.cwd, time, rodada
                ),
                index=False,
                header=True,
                sep=";",
                encoding="utf-8",
            )

            # CAPITAO
            df_c = pd.DataFrame()
            df_c = pd.concat([df_e, dados], ignore_index=True)
            df_c["rodada"] = rodada
            df_c["rodada_id"] = rodada
            df_c["atletas"] = df_c["atletas"].astype(str) + "]"
            df_c["atletas"] = df_c["atletas"].values.tolist()
            df_c = df_c[["rodada", "timetime_id", "capitao_id"]]

            df_c.dropna(subset=["capitao_id"], inplace=True)
            df_c = df_c.astype({"capitao_id": int})

            df_capitao = pd.concat([df_capitao, df_c], ignore_index=True)
            df_capitao = df_capitao.drop_duplicates(subset=["rodada", "timetime_id"])

            df_capitao = df_capitao.sort_values(
                ["rodada", "timetime_id"], ascending=([True, True])
            ).reset_index(drop=True)
            df_capitao = df_capitao.rename(
                columns={
                    "rodada": "rodada_id",
                    "capitao_id": "atleta_id",
                    "timetime_id": "time_id",
                }
            )

            df_capitao.to_csv(
                "{}/DADOS/CAPITAO/capitao_{}_{:0>2}.csv".format(self.cwd, time, rodada),
                index=False,
                header=True,
                sep=";",
                encoding="utf-8",
            )

            # RESERVA DE LUXO
            df_luxo = pd.DataFrame()
            df_luxo = pd.concat([df_e, dados], ignore_index=True)
            df_luxo["rodada"] = rodada
            df_luxo["rodada_id"] = rodada
            df_luxo["atletas"] = df_luxo["atletas"].astype(str) + "]"
            df_luxo["atletas"] = df_luxo["atletas"].values.tolist()
            df_luxo = df_luxo[["rodada", "timetime_id", "reserva_luxo_id"]]

            df_luxo.dropna(subset=["reserva_luxo_id"], inplace=True)
            df_luxo = df_luxo.astype({"reserva_luxo_id": int})

            df_reservaluxo = pd.concat([df_reservaluxo, df_luxo], ignore_index=True)
            df_reservaluxo = df_reservaluxo.drop_duplicates(
                subset=["rodada", "timetime_id"]
            )

            df_reservaluxo = df_reservaluxo.sort_values(
                ["rodada", "timetime_id"], ascending=([True, True])
            ).reset_index(drop=True)
            df_reservaluxo = df_reservaluxo.rename(
                columns={
                    "rodada": "rodada_id",
                    "reserva_luxo_id": "atleta_id",
                    "timetime_id": "time_id",
                }
            )

            df_reservaluxo.to_csv(
                "{}/DADOS/RESERVALUXO/reservaluxo_{}_{:0>2}.csv".format(
                    self.cwd, time, rodada
                ),
                index=False,
                header=True,
                sep=";",
                encoding="utf-8",
            )

            # TITULAR
            body = response.json()["atletas"]
            dados = json_normalize(data=body, sep="")

            df_t = pd.DataFrame()
            df_t = pd.concat([df_t, dados], ignore_index=True)
            # df_t = df_t[["atleta_id"]]
            df_t["rodada"] = rodada
            df_t["rodada_id"] = rodada
            df_t["timetime_id"] = time
            # df_t = df_t[["rodada", "timetime_id", "atleta_id"]]
            df_t.drop(
                ["slug", "apelido", "apelido_abreviado", "nome", "foto"],
                axis=1,
                inplace=True,
            )

            df_t_01 = df_t.copy()
            df_t_01 = df_t_01[
                [
                    "atleta_id",
                    "rodada_id",
                    "clube_id",
                    "posicao_id",
                    "status_id",
                    "pontos_num",
                    "preco_num",
                    "variacao_num",
                    "media_num",
                    "jogos_num",
                    "rodada",
                    "timetime_id",
                ]
            ]

            df_t_02 = df_t.copy()
            df_t_02.drop(
                [
                    "atleta_id",
                    "rodada_id",
                    "clube_id",
                    "posicao_id",
                    "status_id",
                    "pontos_num",
                    "preco_num",
                    "variacao_num",
                    "media_num",
                    "jogos_num",
                    "rodada",
                    "timetime_id",
                ],
                axis=1,
                inplace=True,
            )

            df_t_03 = pd.DataFrame(
                columns=[
                    "scoutG",
                    "scoutA",
                    "scoutFT",
                    "scoutFD",
                    "scoutFF",
                    "scoutFS",
                    "scoutPS",
                    "scoutPP",
                    "scoutI",
                    "scoutPI",
                    "scoutDP",
                    "scoutSG",
                    "scoutDE",
                    "scoutDS",
                    "scoutGC",
                    "scoutCV",
                    "scoutCA",
                    "scoutGS",
                    "scoutFC",
                    "scoutPC",
                    "scoutV",
                ]
            )

            df_t_03 = pd.concat([df_t_03, df_t_02], ignore_index=True)
            df_t_final = pd.merge(df_t_01, df_t_03, left_index=True, right_index=True)

            df_titular = pd.concat([df_titular, df_t_final], ignore_index=True)
            df_titular = df_titular.drop_duplicates(
                subset=["rodada", "timetime_id", "atleta_id"]
            )
            df_titular = df_titular.sort_values(
                ["rodada", "timetime_id"], ascending=([True, True])
            ).reset_index(drop=True)

            df_titular = df_titular.rename(columns={"timetime_id": "time_id"})

            df_titular["scoutG"] = df_titular["scoutG"].fillna("0")
            df_titular["scoutA"] = df_titular["scoutA"].fillna("0")
            df_titular["scoutFT"] = df_titular["scoutFT"].fillna("0")
            df_titular["scoutFD"] = df_titular["scoutFD"].fillna("0")
            df_titular["scoutFF"] = df_titular["scoutFF"].fillna("0")
            df_titular["scoutFS"] = df_titular["scoutFS"].fillna("0")
            df_titular["scoutPS"] = df_titular["scoutPS"].fillna("0")
            df_titular["scoutPP"] = df_titular["scoutPP"].fillna("0")
            df_titular["scoutI"] = df_titular["scoutI"].fillna("0")
            df_titular["scoutPI"] = df_titular["scoutPI"].fillna("0")
            df_titular["scoutDP"] = df_titular["scoutDP"].fillna("0")
            df_titular["scoutSG"] = df_titular["scoutSG"].fillna("0")
            df_titular["scoutDE"] = df_titular["scoutDE"].fillna("0")
            df_titular["scoutDS"] = df_titular["scoutDS"].fillna("0")
            df_titular["scoutGC"] = df_titular["scoutGC"].fillna("0")
            df_titular["scoutCV"] = df_titular["scoutCV"].fillna("0")
            df_titular["scoutCA"] = df_titular["scoutCA"].fillna("0")
            df_titular["scoutGS"] = df_titular["scoutGS"].fillna("0")
            df_titular["scoutFC"] = df_titular["scoutFC"].fillna("0")
            df_titular["scoutPC"] = df_titular["scoutPC"].fillna("0")
            df_titular["scoutV"] = df_titular["scoutV"].fillna("0")

            df_titular = df_titular.astype(
                {
                    "scoutG": "int",
                    "scoutA": "int",
                    "scoutFT": "int",
                    "scoutFD": "int",
                    "scoutFF": "int",
                    "scoutFS": "int",
                    "scoutPS": "int",
                    "scoutPP": "int",
                    "scoutI": "int",
                    "scoutPI": "int",
                    "scoutDP": "int",
                    "scoutSG": "int",
                    "scoutDE": "int",
                    "scoutDS": "int",
                    "scoutGC": "int",
                    "scoutCV": "int",
                    "scoutCA": "int",
                    "scoutGS": "int",
                    "scoutFC": "int",
                    "scoutPC": "int",
                    "scoutV": "int",
                }
            )

            df_titular = df_titular.iloc[:, 0:33]
            df_titular.to_csv(
                "{}/DADOS/TITULAR/titular_{}_{:0>2}.csv".format(self.cwd, time, rodada),
                index=False,
                header=True,
                sep=";",
                encoding="utf-8",
            )

            # RESERVA
            try:
                body = response.json()["reservas"]
                dados = json_normalize(data=body, sep="")
                df_r = pd.DataFrame()

                df_r = pd.concat([df_r, dados], ignore_index=True)
                df_r["rodada"] = rodada
                df_r["rodada_id"] = rodada
                df_r["timetime_id"] = time
                df_r.drop(
                    ["slug", "apelido", "apelido_abreviado", "nome", "foto"],
                    axis=1,
                    inplace=True,
                )

                df_r_01 = df_r.copy()
                df_r_01 = df_r_01[
                    [
                        "atleta_id",
                        "rodada_id",
                        "clube_id",
                        "posicao_id",
                        "status_id",
                        "pontos_num",
                        "preco_num",
                        "variacao_num",
                        "media_num",
                        "jogos_num",
                        "rodada",
                        "timetime_id",
                    ]
                ]

                df_r_02 = df_r.copy()
                df_r_02.drop(
                    [
                        "atleta_id",
                        "rodada_id",
                        "clube_id",
                        "posicao_id",
                        "status_id",
                        "pontos_num",
                        "preco_num",
                        "variacao_num",
                        "media_num",
                        "jogos_num",
                        "rodada",
                        "timetime_id",
                    ],
                    axis=1,
                    inplace=True,
                )

                df_r_03 = pd.DataFrame(
                    columns=[
                        "scoutG",
                        "scoutA",
                        "scoutFT",
                        "scoutFD",
                        "scoutFF",
                        "scoutFS",
                        "scoutPS",
                        "scoutPP",
                        "scoutI",
                        "scoutPI",
                        "scoutDP",
                        "scoutSG",
                        "scoutDE",
                        "scoutDS",
                        "scoutGC",
                        "scoutCV",
                        "scoutCA",
                        "scoutGS",
                        "scoutFC",
                        "scoutPC",
                        "scoutV",
                    ]
                )

                df_r_03 = pd.concat([df_r_03, df_r_02])
                df_r_final = pd.merge(
                    df_r_01, df_r_03, left_index=True, right_index=True
                )

                df_reserva = pd.concat([df_reserva, df_r_final])

                df_reserva = df_reserva.drop_duplicates(
                    subset=["rodada", "timetime_id", "atleta_id"]
                )
                df_reserva = df_reserva.sort_values(
                    ["rodada", "timetime_id"], ascending=([True, True])
                ).reset_index(drop=True)

                df_reserva = df_reserva.rename(columns={"timetime_id": "time_id"})

                df_reserva["scoutG"] = df_reserva["scoutG"].fillna("0")
                df_reserva["scoutA"] = df_reserva["scoutA"].fillna("0")
                df_reserva["scoutFT"] = df_reserva["scoutFT"].fillna("0")
                df_reserva["scoutFD"] = df_reserva["scoutFD"].fillna("0")
                df_reserva["scoutFF"] = df_reserva["scoutFF"].fillna("0")
                df_reserva["scoutFS"] = df_reserva["scoutFS"].fillna("0")
                df_reserva["scoutPS"] = df_reserva["scoutPS"].fillna("0")
                df_reserva["scoutPP"] = df_reserva["scoutPP"].fillna("0")
                df_reserva["scoutI"] = df_reserva["scoutI"].fillna("0")
                df_reserva["scoutPI"] = df_reserva["scoutPI"].fillna("0")
                df_reserva["scoutDP"] = df_reserva["scoutDP"].fillna("0")
                df_reserva["scoutSG"] = df_reserva["scoutSG"].fillna("0")
                df_reserva["scoutDE"] = df_reserva["scoutDE"].fillna("0")
                df_reserva["scoutDS"] = df_reserva["scoutDS"].fillna("0")
                df_reserva["scoutGC"] = df_reserva["scoutGC"].fillna("0")
                df_reserva["scoutCV"] = df_reserva["scoutCV"].fillna("0")
                df_reserva["scoutCA"] = df_reserva["scoutCA"].fillna("0")
                df_reserva["scoutGS"] = df_reserva["scoutGS"].fillna("0")
                df_reserva["scoutFC"] = df_reserva["scoutFC"].fillna("0")
                df_reserva["scoutPC"] = df_reserva["scoutPC"].fillna("0")
                df_reserva["scoutV"] = df_reserva["scoutV"].fillna("0")

                df_reserva = df_reserva.astype(
                    {
                        "scoutG": "int",
                        "scoutA": "int",
                        "scoutFT": "int",
                        "scoutFD": "int",
                        "scoutFF": "int",
                        "scoutFS": "int",
                        "scoutPS": "int",
                        "scoutPP": "int",
                        "scoutI": "int",
                        "scoutPI": "int",
                        "scoutDP": "int",
                        "scoutSG": "int",
                        "scoutDE": "int",
                        "scoutDS": "int",
                        "scoutGC": "int",
                        "scoutCV": "int",
                        "scoutCA": "int",
                        "scoutGS": "int",
                        "scoutFC": "int",
                        "scoutPC": "int",
                        "scoutV": "int",
                    }
                )

                df_reserva = df_reserva.iloc[:, 0:33]
                df_reserva.to_csv(
                    "{}/DADOS/RESERVA/reserva_{}_{:0>2}.csv".format(
                        self.cwd, time, rodada
                    ),
                    index=False,
                    header=True,
                    sep=";",
                    encoding="utf-8",
                )

            except KeyError:
                print("Time {} sem reservas na rodada {}".format(time, rodada))

        async def main():
            async with httpx.AsyncClient(limits=self._limits, timeout=None) as client:
                tasks = []
                for rodada in range(1, rodadas + 1):
                    for time in lista_liga:
                        auth_url = (
                            "https://api.cartolafc.globo.com/time/id/{}/{}".format(
                                time, rodada
                            )
                        )
                        tasks.append(
                            asyncio.ensure_future(
                                get_escalacao(client, auth_url, time, rodada)
                            )
                        )
                await asyncio.gather(*tasks)

        asyncio.run(main())
        self.tac()


# %%
