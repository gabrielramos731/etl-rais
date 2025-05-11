#!/bin/bash

# URL base do FTP
FTP_HOST="ftp://ftp.mtps.gov.br/pdet/microdados/RAIS"

# Pasta local para salvar os dados
DESTINO="./baixados_rais"
mkdir -p "$DESTINO"

# Obter lista de diretórios (com barra no final) e salvar
lftp "$FTP_HOST" -e "
cls -1
bye
" > pastas.txt

# Filtrar apenas pastas com nome de ano >= 2007 (tirando a barra final)
while read pasta; do
    ano="${pasta%/}"  # remove a barra final
    if [[ "$ano" =~ ^[0-9]{4}$ ]] && [[ "$ano" -ge 2007 ]]; then
        echo "Baixando: $pasta"
        lftp "$FTP_HOST" -e "
mirror --continue --verbose $pasta $DESTINO/$ano
bye
"
    fi
done < pastas.txt

rm -f pastas.txt
echo "Todos os downloads foram concluídos!"
