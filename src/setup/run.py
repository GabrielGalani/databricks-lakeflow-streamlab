import os

def executar_script_sql(caminho_arquivo):
    """
    Executa comandos SQL de um arquivo e exibe o resultado de cada um na tela.
    """
    try:
        with open(caminho_arquivo, "r") as f:
            conteudo = f.read()
        comandos = [cmd.strip() for cmd in conteudo.split(';') if cmd.strip()]

        print(f"🚀 Iniciando script: {caminho_arquivo}\n" + "="*50)
        
        for i, sql in enumerate(comandos, 1):
            print(f"\n#️⃣ Executando comando {i}/{len(comandos)}:")
            print(f"SQL: {sql[:100]}..." if len(sql) > 100 else f"SQL: {sql}")
            
            df_resultado = spark.sql(sql)
            
            if df_resultado.isStreaming:
                 print("Streaming iniciado...")
            else:
                if df_resultado.count() != 0:
                    df_resultado.show(truncate=False)
                
        print("\n" + "="*50 + "\n✅ Todos os comandos foram executados!")
        print("\n" +f"🎉 Script {caminho_arquivo} concluído com sucesso!")


    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        raise e

if __name__ == "__main__":
    arquivos = [f for f in os.listdir(".") if f.endswith('.sql')]
    arquivos_ordenados = sorted(arquivos)

    print(f"📂 Pasta atual: {os.getcwd()}")

    for arquivo in arquivos_ordenados:
        executar_script_sql(os.path.join(os.getcwd(), arquivo))









