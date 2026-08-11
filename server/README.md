# FileSend — Compartilhamento de arquivos entre redes diferentes (com criptografia ponta-a-ponta)

Duas formas de rodar isso, escolha a que fizer mais sentido pra você:

| | **Opção A: `1-koyeb/client/` + `1-koyeb/server/`** | **Opção B: `2-fileio/client/`** |
|---|---|---|
| Infraestrutura | Você hospeda um servidor próprio (Koyeb) | Nenhuma — usa a API pública do file.io |
| Apaga após download | Sim (implementado no servidor) | Sim (nativo do file.io, plano grátis) |
| Limite de tamanho | Você define (200MB por padrão) | 2GB por arquivo, 4GB de upload/hora |
| Dependência de terceiros | Nenhuma | Sim, depende do file.io existir e funcionar |
| Controle/transparência | Total — o código é todo seu | Você confia num serviço externo (mesmo cifrado) |

A criptografia (`crypto_utils.py`, AES-256-GCM) é **idêntica nas duas opções** — só muda pra onde o arquivo cifrado é enviado.

---

## Opção B: usando o file.io (sem servidor próprio)

Pasta `2-fileio/client/`. Mais simples de colocar pra rodar porque não tem servidor pra hospedar.

### Como funciona

```
[Computador A]                                              [Computador B]
   arquivo original                                        arquivo original
        │ cifra localmente (AES-256-GCM)                          ▲
        ▼                                                         │ decifra localmente
   arquivo.enc  --upload-->  [file.io]  --download-->          arquivo.enc
                  (só vê bytes opacos,
                   nunca a chave, apaga
                   sozinho após 1 download)
```

1. No computador A: seleciona o arquivo, clica em **Cifrar e enviar**. Mesmo processo de cifragem local de sempre, só que o upload vai direto para `https://file.io` em vez do seu servidor.
2. O file.io devolve um `key` curto (ex: `WdYTUJ`). O app combina isso com a chave de cifragem e monta o código completo (ex: `WdYTUJ.k3jX9f...`).
3. Você compartilha esse código.
4. No computador B: cola o código, clica em **Baixar e decifrar**. Diferente da versão com servidor próprio, o file.io não tem um endpoint público para "consultar nome/tamanho antes de baixar" sem autenticação — então aqui o download começa direto, sem etapa de prévia.
5. O file.io **apaga o arquivo automaticamente depois do primeiro download** — é o comportamento padrão do plano gratuito deles, não precisei programar nada pra isso.

### Rodando

```bash
cd 2-fileio/client
pip install -r requirements.txt
python app.py
```

Não precisa configurar URL de servidor nem fazer deploy de nada — já funciona de primeira, uso anônimo (sem conta no file.io).

### Limitações desta opção

- **Sem conta**: 2GB por arquivo, 4GB de upload por hora, um download por arquivo. Suficiente pro seu caso de uso (10-200MB).
- **Sem prévia**: como não dá pra consultar metadados sem baixar, se o código estiver errado ou expirado você só descobre ao tentar baixar (o erro aparece claro, mas não tem a etapa de "conferir antes").
- **Dependência de terceiro**: se o file.io mudar a API, ficar fora do ar, ou impuser mais restrições no futuro, essa versão para de funcionar até você ajustar o código. Como o conteúdo sempre viaja cifrado, isso é um risco de disponibilidade, não de confidencialidade.
- Quer mais controle, arquivos maiores, ou zero dependência externa? Use a **Opção A** (`1-koyeb/client/` + `1-koyeb/server/`) abaixo.

---

## Opção A: servidor próprio na Koyeb

Aplicação com duas partes:

- **`1-koyeb/server/`** — API relay (FastAPI) que fica hospedada na nuvem (Koyeb) e serve de ponte entre os dois computadores, sem precisar que nenhum dos dois esteja na mesma rede ou abra portas.
- **`1-koyeb/client/`** — App desktop (CustomTkinter) para enviar e receber arquivos usando essa API, cifrando tudo localmente com AES-256-GCM.

## Como funciona

```
[Computador A]                                                        [Computador B]
   arquivo original                                                  arquivo original
        │  cifra localmente (AES-256-GCM)                                  ▲
        ▼                                                                  │  decifra localmente
   arquivo.enc  --upload-->  [Servidor relay na Koyeb]  --download-->  arquivo.enc
                              (só vê bytes opacos,
                               nunca a chave)
```

1. No computador A, você seleciona um arquivo e clica em **Cifrar e enviar**. O app:
   - gera uma chave AES-256 aleatória,
   - cifra o arquivo em blocos (streaming, não carrega tudo na memória),
   - sobe o arquivo já cifrado para o servidor com nome genérico (`arquivo.enc`),
   - gera um **código combinado** tipo `DYB7RG.mK3dv1h8OBO992g7LLP0_tt-BmxYq6OmOHovLGrA1Lw` (código do servidor + chave, separados por ponto).
2. Você manda esse código completo pro computador B por qualquer canal.
3. No computador B, a pessoa cola o código na aba **Receber**, clica em **Baixar e decifrar**. O app baixa o `.enc`, decifra localmente e só então pergunta onde salvar — já sugerindo o nome original do arquivo (recuperado de dentro do conteúdo cifrado).
4. **Assim que o download termina de ser entregue, o servidor apaga o arquivo automaticamente.** O código passa a ser de uso único — se alguém tentar usá-lo de novo, recebe erro de "código não encontrado". Isso é feito no próprio servidor (não depende do cliente avisar que terminou), então funciona mesmo se o download for interrompido por outro motivo depois de completo, ou se alguém tentar reusar o código maliciosamente.

Esse comportamento pode ser desligado (por exemplo se você quiser permitir múltiplos downloads do mesmo código dentro da janela de expiração) definindo `DELETE_AFTER_DOWNLOAD=false` nas variáveis de ambiente do servidor na Koyeb.

**O servidor relay nunca tem acesso à chave nem ao conteúdo original** — ele só armazena e transporta bytes cifrados com nome genérico. Mesmo alguém com acesso total ao servidor (ou interceptando o tráfego, mesmo sem HTTPS) não consegue ler o arquivo sem o código completo.

> ⚠️ Importante: o código combinado é a "chave da casa" — quem tiver o código completo consegue abrir o arquivo. Para máxima segurança, evite mandar código e contexto sensível pelo mesmo canal que pode ser interceptado (ex: se o e-mail da pessoa já estiver comprometido, um WhatsApp separado é mais seguro). Para uso interno da prefeitura isso já é uma proteção sólida contra acesso indevido ao servidor ou a terceiros no meio do caminho.

Como os dois clientes só falam com o servidor (nunca um com o outro diretamente), funciona mesmo que estejam em redes com CGNAT, 4G, Wi-Fi corporativo bloqueado, etc.

## 1. Subindo o servidor na Koyeb

1. Crie uma conta gratuita em https://www.koyeb.com se ainda não tiver.
2. Suba a pasta `1-koyeb/server/` para um repositório no GitHub (pode ser privado).
3. Na Koyeb: **Create Service → Deploy from GitHub** → selecione o repositório.
4. A Koyeb detecta o `Dockerfile` automaticamente. Configure:
   - **Port**: `8000`
   - **Variáveis de ambiente (opcional)**:
     - `MAX_FILE_SIZE_MB` (padrão 200)
     - `EXPIRATION_HOURS` (padrão 6)
     - `DELETE_AFTER_DOWNLOAD` (padrão `true` — apaga o arquivo assim que o download termina)
5. Deploy. A Koyeb te dá uma URL pública tipo `https://filesend-eddie.koyeb.app`.
6. Teste no navegador: `https://sua-url.koyeb.app/health` deve responder `{"status":"ok",...}`.

> Alternativa rápida para testar localmente antes de subir: rode `python main.py` dentro de `1-koyeb/server/` e use `http://localhost:8000` no cliente.

## 2. Rodando o cliente

```bash
cd 1-koyeb/client
pip install -r requirements.txt
python app.py
```

Na primeira execução, vá na aba **Configurações** e cole a URL do servidor Koyeb. Isso é salvo em `1-koyeb/client/config.json` — depois disso é só abrir o app normalmente.

## 3. Gerando um executável (opcional)

Para distribuir o cliente sem precisar instalar Python nos outros computadores:

```bash
pip install pyinstaller
pyinstaller --noconsole --onefile --name FileSend app.py
```

O executável fica em `client/dist/FileSend.exe` (Windows) ou equivalente no Linux/Mac. O `config.json` precisa estar na mesma pasta do executável.

## Detalhes técnicos da criptografia

- Algoritmo: **AES-256-GCM** (autenticado — além de sigilo, garante que o arquivo não foi alterado no caminho).
- Cifragem em **chunks de 1MB**, cada um com nonce único (contador + bytes aleatórios), então arquivos grandes não precisam caber inteiros na memória.
- O nome original e o tamanho do arquivo também vão dentro do conteúdo cifrado (um "header" cifrado no início do arquivo `.enc`) — o servidor só vê `arquivo.enc`, nunca o nome real.
- Se a chave estiver errada, a decifragem falha explicitamente (GCM detecta adulteração/chave incorreta) em vez de gerar um arquivo corrompido silenciosamente.
- Implementado em `client/crypto_utils.py`, isolado da lógica de interface — pode ser reaproveitado em outros projetos seus (ex: UniRota já usa Fernet para campos de banco; aqui é AES-GCM em streaming, mais adequado para arquivos grandes).

## Limitações e próximos passos possíveis

- Arquivos grandes (200MB+) funcionam, mas o tempo de upload/download depende da velocidade de internet de cada ponta.
- O código combinado (código + chave) precisa ser copiado por inteiro — se cortar um pedaço, a decifragem falha com erro claro em vez de dar arquivo corrompido silencioso.
- O armazenamento no servidor é em disco local do container — no plano gratuito da Koyeb isso é efêmero entre deploys, mas persiste durante a execução normal, que é o que importa pro caso de uso (arquivo cifrado vive poucas horas).
- Se quiser, dá pra evoluir para um bucket S3-compatível (ex: Cloudflare R2) para mais durabilidade, sem mudar nada do modelo de criptografia — o servidor continua só manipulando bytes opacos.

