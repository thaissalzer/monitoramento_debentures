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

PASTA_PROJETO = os.getcwd()
PASTA_DOWNLOADS = os.path.join(PASTA_PROJETO, "dados_cvm_diarios")
os.makedirs(PASTA_DOWNLOADS, exist_ok=True)

URL_ZIP = "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"
ARQUIVO_ZIP_LOCAL = os.path.join(PASTA_DOWNLOADS, "oferta_distribuicao.zip")
ARQUIVOS_CSV_PARA_EXTRAIR = ["oferta_distribuicao.csv", "oferta_resolucao_160.csv"]
ARQUIVO_CVM_PARA_PROCESSAR = os.path.join(PASTA_DOWNLOADS, "oferta_resolucao_160.csv")
ARQUIVO_DEB_PROCESSADAS = os.path.join(PASTA_PROJETO, "deb_processadas.csv")

# Configurações de E-mail
EMAIL_REMETENTE = "monitoramentodebentures@gmail.com"
SENHA_APP_EMAIL = "syqz kjjy rpfe pbxg"  # Senha de app do Gmail
EMAILS_DESTINATARIOS = [
    "thaissalzer@gmail.com",
    "thais.procopio@fazenda.gov.br",
    "carlos.colombo@fazenda.gov.br",
    "alberto.oliveira@fazenda.gov.br",
    "henrique.arakawa@fazenda.gov.br",
    "monitoramentodebentures@fazenda.gov.br"
]
SERVIDOR_SMTP = "smtp.gmail.com"
PORTA_SMTP = 587

# ======================
# 🔽 Funções
# ======================

def log(mensagem):
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{agora}] {mensagem}")

def baixar_arquivo_zip():
    log("Baixando arquivo ZIP da CVM...")
    response = requests.get(URL_ZIP, stream=True, timeout=60)
    response.raise_for_status()

    with open(ARQUIVO_ZIP_LOCAL, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    log(f"Arquivo ZIP salvo em: {ARQUIVO_ZIP_LOCAL}")

def descompactar_zip():
    log("Descompactando arquivo ZIP...")
    with zipfile.ZipFile(ARQUIVO_ZIP_LOCAL, 'r') as zip_ref:
        for arquivo in ARQUIVOS_CSV_PARA_EXTRAIR:
            if arquivo in zip_ref.namelist():
                zip_ref.extract(arquivo, PASTA_DOWNLOADS)
                log(f"Arquivo extraído: {arquivo}")
            else:
                log(f"Aviso: {arquivo} não encontrado no ZIP")

def ler_csv(caminho_csv):
    try:
        df = pd.read_csv(caminho_csv, sep=";", encoding="latin1", low_memory=False)
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        log(f"ERRO ao ler CSV {caminho_csv}: {e}")
        return pd.DataFrame()

def processar_e_comparar_dados():
    df_novo = ler_csv(ARQUIVO_CVM_PARA_PROCESSAR)

    if df_novo.empty:
        log("CSV da CVM está vazio ou com erro.")
        return pd.DataFrame()

    df_filtrado = df_novo[
        (df_novo["Valor_Mobiliario"] == "Debêntures") & 
        (df_novo["Titulo_incentivado"] == "S")
    ].copy()

    log(f"{len(df_filtrado)} linhas filtradas de debêntures incentivadas.")

    df_existente = ler_csv(ARQUIVO_DEB_PROCESSADAS)
    chaves = ["Numero_Requerimento", "Numero_Processo"]

    if not df_existente.empty:
        df_merged = pd.merge(
            df_filtrado,
            df_existente,
            on=chaves,
            how="left",
            indicator=True
        )
        novas_entradas = df_merged[df_merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        novas_entradas = df_filtrado.copy()

    log(f"{len(novas_entradas)} novas entradas detectadas.")

    df_atualizado = pd.concat([df_existente, df_filtrado], ignore_index=True)
    df_atualizado.drop_duplicates(subset=chaves, inplace=True)
    df_atualizado.to_csv(ARQUIVO_DEB_PROCESSADAS, index=False, sep=";", encoding="latin1")
    log(f"Arquivo de controle atualizado com {len(df_atualizado)} entradas.")

    return novas_entradas

def enviar_email_alerta(novas_entradas):
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
    msg["To"] = ", ".join(EMAILS_DESTINATARIOS)

    try:
        with smtplib.SMTP(SERVIDOR_SMTP, PORTA_SMTP) as server:
            server.starttls()
            server.login(EMAIL_REMETENTE, SENHA_APP_EMAIL)
            server.sendmail(EMAIL_REMETENTE, EMAILS_DESTINATARIOS, msg.as_string())
        log(f"E-mail enviado para {', '.join(EMAILS_DESTINATARIOS)}.")
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
            log(f"{len(novas)} novas debêntures encontradas.")
        else:
            log("Nenhuma nova debênture encontrada.")
    except Exception as e:
        log(f"ERRO FATAL: {e}")
