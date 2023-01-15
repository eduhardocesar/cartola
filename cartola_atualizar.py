import cartola_api

useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"

# Para obter o 'globoid' e o 'token' de sua conta acesse manualmente no cartola em seu navegador,
# depois de logado entre no seguinte endereço: https://login.globo.com/api/user
# ao entrar no endereço receberá o retorno a api da globo com os dados de sua conta
# globoid = globoId e token = glbId

globoid = ""
token = ""

# Insira o nome dos times de sua liga que deseja obter informações
times = []

diretorio = "Insira aqui o seu diretorio armazenamento do código e bases que serão extaidas da API"

cartola_api.api(useragent, globoid, token, times, diretorio)
