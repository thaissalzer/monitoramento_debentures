# automacao_cvm.py

import requests
import zipfile
import pandas as pd
import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# ======================
# 🔧 CONFIGURAÇÕES
# ======================

# Gerenciamento de pastas
try:
    PASTA_PROJETO = os.path.dirname(os.path.abspath(__file__))
except NameError:
    PASTA_PROJETO = os.getcwd()

PASTA_DOWNLOADS = os.path.join(PASTA_PROJETO, "dados_cvm_diarios")
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)

# URLs e arquivos
URL_ZIP = "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"
ARQUIVO_ZIP_LOCAL = os.path.join(PASTA_DOWNLOADS, "oferta_distribuicao.zip")
ARQUIVOS_CSV_PARA_EXTRAIR = ["oferta_distribuicao.csv", "oferta_resolucao_160.csv"]
ARQUIVO_CVM_PARA_PROCESSAR = os.path.join(PASTA_DOWNLOADS, "oferta_resolucao_160.csv")
ARQUIVO_DEB_PROCESSADAS = os.path.join(PASTA_PROJETO, "deb_processadas.csv")

# E-mail
EMAIL_REMETENTE = "python.para.negocios@gmail.com"
SENHA_APP_EMAIL = "kool csfe venh yqfs"  # Senha de app do Gmail
EMAIL_DESTINATARIO = "thaissalzer@gmail.com"
SERVIDOR_SMTP = "smtp.gmail.com"
PORTA_SMTP = 587

# Filtros
COLUNA_TIPO_VALOR_MOBILIARIO = "Valor_Mobiliario"
COLUNA_INCENTIVADO = "Titulo_incentivado"
COLUNAS_CHAVE_UNICA = ["Numero_Requerimento", "Numero_Processo"]


# ======================
# 🔽 Funções
# ======================

def log(mensagem):
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{agora}] {mensagem}")


def baixar_arquivo_zip():
    """Baixa o ZIP da CVM."""
    log("Baixando arquivo ZIP da CVM...")
    try:
        response = requests.get(URL_ZIP, stream=True, timeout=60)
        response.raise_for_status()

        with open(ARQUIVO_ZIP_LOCAL, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        log(f"Arquivo ZIP salvo em: {ARQUIVO_ZIP_LOCAL}")
    except Exception as e:
        log(f"ERRO ao baixar o arquivo: {e}")
        raise


def descompactar_zip():
    """Extrai CSVs do ZIP."""
    log("Descompactando arquivo ZIP...")
    try:
        with zipfile.ZipFile(ARQUIVO_ZIP_LOCAL, 'r') as zip_ref:
            for arquivo in ARQUIVOS_CSV_PARA_EXTRAIR:
                if arquivo in zip_ref.namelist():
                    zip_ref.extract(arquivo, PASTA_DOWNLOADS)
                    log(f"Arquivo extraído: {arquivo}")
                else:
                    log(f"Aviso: {arquivo} não encontrado no ZIP")
    except Exception as e:
        log(f"ERRO ao descompactar: {e}")
        raise


def ler_csv(caminho_csv):
    """Lê CSV com tratamento."""
    try:
        df = pd.read_csv(caminho_csv, sep=";", encoding="latin1", low_memory=False)
        df.columns = df.columns.str.strip()  # Remove espaços
        return df
    except Exception as e:
        log(f"ERRO ao ler CSV {caminho_csv}: {e}")
        return pd.DataFrame()


def processar_e_comparar_dados():
    """Processa CSV da CVM, filtra debêntures incentivadas e compara com histórico."""
    df_novo = ler_csv(ARQUIVO_CVM_PARA_PROCESSAR)

    if df_novo.empty:
        log("CSV da CVM está vazio ou com erro.")
        return pd.DataFrame()

    # Verificar colunas necessárias
    for coluna in [COLUNA_TIPO_VALOR_MOBILIARIO, COLUNA_INCENTIVADO]:
        if coluna not in df_novo.columns:
            log(f"ERRO: Coluna '{coluna}' não encontrada no CSV da CVM.")
            return pd.DataFrame()

    # Filtrar debêntures incentivadas
    df_filtrado = df_novo[
        (df_novo[COLUNA_TIPO_VALOR_MOBILIARIO] == "Debêntures") &
        (df_novo[COLUNA_INCENTIVADO] == "S")
    ].copy()

    log(f"{len(df_filtrado)} linhas filtradas de debêntures incentivadas.")

    # Ler base existente (se houver)
    df_existente = ler_csv(ARQUIVO_DEB_PROCESSADAS)

    # Verificar colunas chave
    for coluna in COLUNAS_CHAVE_UNICA:
        if coluna not in df_filtrado.columns:
            log(f"ERRO: Coluna chave '{coluna}' ausente no CSV novo.")
            return pd.DataFrame()
        if not df_existente.empty and coluna not in df_existente.columns:
            log(f"ERRO: Coluna chave '{coluna}' ausente no CSV existente.")
            return pd.DataFrame()

    # Comparar e detectar novas entradas
    if not df_existente.empty:
        df_merged = pd.merge(
            df_filtrado,
            df_existente,
            on=COLUNAS_CHAVE_UNICA,
            how="left",
            indicator=True
        )
        novas_entradas = df_merged[df_merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        novas_entradas = df_filtrado.copy()

    log(f"{len(novas_entradas)} novas entradas detectadas.")

    # Atualizar CSV de controle
    df_atualizado = pd.concat([df_existente, df_filtrado], ignore_index=True)
    df_atualizado.drop_duplicates(subset=COLUNAS_CHAVE_UNICA, inplace=True)
    df_atualizado.to_csv(ARQUIVO_DEB_PROCESSADAS, index=False, sep=";", encoding="latin1")
    log(f"Arquivo de controle atualizado com {len(df_atualizado)} entradas.")

    return novas_entradas


def enviar_email_alerta(novas_entradas):
    """Envia e-mail com alerta das novas entradas."""
    if novas_entradas.empty:
        log("Nenhuma nova entrada. E-mail não será enviado.")
        return

    log("Enviando e-mail de alerta...")

    assunto = f"📢 Novas Debêntures Incentivadas na CVM ({len(novas_entradas)} novas)"
    coluna_link = "Numero_Requerimento"
    base_link = "https://web.cvm.gov.br/sre-publico-cvm/#/oferta-publica/"

    detalhes = ""
    if coluna_link in novas_entradas.columns:
        links = [
            f"{base_link}{str(num)}"
            for num in novas_entradas[coluna_link].dropna().astype(str).head(10)
        ]
        detalhes = "\n".join(links)
    else:
        detalhes = novas_entradas.head(10).to_string()

    corpo = f"""
Prezados,

Foram detectadas {len(novas_entradas)} novas ofertas de debêntures com incentivo fiscal na CVM.

Detalhes:
{detalhes}

Atenciosamente,
Automação CVM
"""

    msg = MIMEText(corpo, "plain", "utf-8")
    msg["Subject"] = assunto
    msg["From"] = EMAIL_REMETENTE
    msg["To"] = EMAIL_DESTINATARIO

    try:
        with smtplib.SMTP(SERVIDOR_SMTP, PORTA_SMTP) as server:
            server.starttls()
            server.login(EMAIL_REMETENTE, SENHA_APP_EMAIL)
            server.send_message(msg)
        log(f"E-mail enviado para {EMAIL_DESTINATARIO}.")
    except Exception as e:
        log(f"ERRO ao enviar e-mail: {e}")


# ======================
# 🚀 EXECUÇÃO
# ======================

if __name__ == "__main__":
    try:
        baixar_arquivo_zip()
        descompactar_zip()
        novas = processar_e_comparar_dados()
        if not novas.empty:
            enviar_email_alerta(novas)
        else:
            log("Nenhuma nova debênture encontrada. Processo concluído.")
    except Exception as e:
        log(f"ERRO FATAL: {e}")
