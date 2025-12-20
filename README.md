Repository dedicated to MVP Project for Data Engineering sprint for PUC-Rio Data Science and Analytics Post Graduate course.


# MVP Data Engineering: Pipeline Airbnb Rio de Janeiro

Este projeto é focado na construção de um pipeline de dados completo utilizando a plataforma **Databricks**. O notebook com todo o passo a passo do MVP está disponível neste repositório e pode ser acessado através deste [link](https://github.com/juliafarah/MVP_Data_Engineering/blob/main/MVP_Pipeline_Airbnb_Rio.ipynb).

Ferramentas utilizadas:
* **Databricks** (Plataforma de Dados)
* **PySpark** (Processamento Distribuído)
* **Python** (Linguagem de Programação)
* **SQL** (Consultas e Análises)
* **Pandas/Matplotlib** (Visualização Gráfica)

---

## 1. Objetivo e Definição do Problema

O objetivo é processar dados públicos do Airbnb referentes à cidade do Rio de Janeiro, transformando dados brutos em insights valiosos sobre o mercado de aluguel por temporada através da **Arquitetura Medalhão** aliada à **Modelagem Star Schema**.

**Perguntas a serem respondidas:**
1. Qual a região do Rio tem a média de preço (ticket médio) mais cara da cidade?
2. Quais são os 10 bairros com a maior disponibilidade de anúncios?
3. Quais regiões da cidade têm mais disponibilidade de apto inteiro, de apenas quarto inteiro e quarto compartilhado?
4. A plataforma está dominada por empresas especializadas ou por hosts que visam apenas ter uma renda extra?
5. Quais bairros garantem maior taxa de ocupação?

---

## 2. Coleta de Dados

Os dados foram extraídos do portal **Inside Airbnb**, uma fonte independente e pública que disponibiliza dados da plataforma Airbnb.

* **Dataset:** `listings.csv`
* **Fonte** [Inside Airbnb - Get the Data - Rio de Janeiro, Brasil](http://insideairbnb.com/get-the-data.html)
* **Descrição dos atributos:**
  | Coluna | Descrição |
  | :--- | :--- |
  | id | Identificador do anúncio. |
  | name | Título do anúncio. |
  | host_id | Identificador do host (anfitrião). |
  | host_name | Nome do host. |
  | neighbourhood_group | Região em que o bairro está inserido. |
  | neighbourhood | Nome do bairro. |
  | latitude | Latitude geográfica. |
  | longitude | Longitude geográfica. |
  | room_type | O tipo da acomodação. |
  | price | O valor da acomodação. |
  | minimum_nights | Qtd mínimo de noites para reserva. |
  | number_of_reviews | Quantos reviews (avaliações) a acomodação tem. |
  | last_review | Data da última review feita. |
  | reviews_per_month | Qtd de reviews por mês. |
  | calculated_host_listings_count | Qtd de anúncios que o host (anfitrião) tem na cidade. |
  | availability_365 | Qtd de dias que a acomodação está disponível nos próximos 365 dias. |
  | number_of_reviews_ltm | Qtd de reviews que o anúncio tem nos últimos 12 meses. |
  | license | Número de registro. |



O CSV com lista dos bairros e regiões da cidade do Rio de Janeiro foi criado a partir dos dados encontrados no site **Estados e Capitais do Brasil**.

* **Dataset:** `neighbourhoods.csv`
* **Fonte:** [Lista dos bairros da cidade do Rio de Janeiro por região](https://www.estadosecapitaisdobrasil.com/lista-dos-bairros-do-rio-de-janeiro/)
* **Descrição dos atributos:**
  | Coluna | Descrição |
  | :--- | :--- |
  | id_bairros | Identificador do bairro |
  | bairro | Nome do bairro |
  | zona | Região da cidade *(central, oeste, sul e norte)* |


**Metodologia de Coleta:**

Após isso, conectou-se os dois datasets na camada inicial do Data Lake a partir da leitura dos caminhos dos arquivos no Databricks Volumes. 

Ambos arquivos também estão disponiveis neste repositório.

---

## 3. Modelagem e Arquitetura de Dados

O projeto foi construido combinando o fluxo de dados da **Arquitetura Medalhão** com a estruturação lógica em **Star Schema** desde o princípio.

### A. Star Schema

A principal estratégia foi transformar a tabela original `listings` (uma tabela *flat* com todas as informações misturadas) em um modelo relacional analítico composto por **1 Tabela Fato e 3 Tabelas Dimensão**. 

Abaixo, o *Entity Relationship Diagram* do Databricks da camada gold mostra a relação entre as tabelas dimensão e tabela fato assim como as Primary e Foreign Keys.


<img width="1052" height="830" alt="image" src="https://github.com/user-attachments/assets/45faa9aa-8b28-4af8-8cb0-5ff6c3dae6f8" />


### B. Arquitetura Medalhão

* **Camada Bronze:** Dados brutos da tabela *flat*, mantendo os dados originais inalterados.

  | **database** | **tableName** | **isTemporary** |
  | :--- | :--- | :--- |
  | bronze | listings | false |
  | bronze | zonas_bairros | false |

* **Camada Silver:** Etapa onde ocorre a limpeza e a partição da tabela *flat*.
    * Separação dos atributos nas 3 Dimensões (Location, Host, Ad).
    * Definição da Tabela Fato com as métricas quantitativas (FK).
    * Tratar o nan convertendo para um nulo real do banco de dados.
    * Remoção de dados que corrompem a análise dos dados.
    * Certificação de que as FKs sejam valores não nulos.


  | **database** | **tableName** | **isTemporary** |
  | :--- | :--- | :--- |
  | silver | dim_ad | false |
  | silver | dim_host | false |
  | silver | dim_location | false |
  | silver | fact_listings | false |
  
* **Camada Gold:** Tabelas Fato e Dimensões consolidadas e otimizadas para performance em ferramentas de BI e análises exploratória dos dados.

  | **database** | **tableName** | **isTemporary** |
  | :--- | :--- | :--- |
  | gold | dim_ad | false |
  | gold | dim_host | false |
  | gold | dim_location | false |
  | gold | fact_listings | false |


Essa abordagem garante integridade referencial e facilita respostas rápidas para as perguntas de negócio definidas no início do projeto.

---

## 4. Pipeline de Carga e Transformação (ETL)

O processo de ETL foi desenvolvido utilizando **PySpark** no Databricks Community/Free Edition, orientado à construção do modelo dimensional:

1.  **Extração (Bronze):** Leitura do arquivo CSV bruto com inferência de schema inicial.
2.  **Transformação e Modelagem (Silver):**
    * Decomposição da tabela flat original.
    * **Criação das Dimensões:** Seleção e tratamento de atributos descritivos (bairros, dados do host, tipo de propriedade).
    * **Criação da Fato:** Isolamento de métricas transacionais (preço, disponibilidade, reviews) e chaves estrangeiras.
    * Limpeza de caracteres especiais (ex: moeda) e conversão de tipos (String -> Double/Int).
3.  **Carga (Gold):**
    * Persistência do Star Schema (Fato + 3 Dimensões) pronto para consumo.
    * Criação de visões agregadas para responder às perguntas de negócio.


---

## 5. Qualidade de Dados

* Verificação de valores nulos em campos críticos como `host_id` e `ad_id`.
* Identificação de nulos, preços negativos, orphan records e etc.
* Garantia de que os dados categóricos (bairros e regiões) estavam padronizados nas dimensões.


---

## 6. Catálogo de Dados prontos para Análise dos Dados (Camada Gold)

### `fact_listings`

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| ad_id | String | Chave estrangeira para a dimensão anúncio (FK) |
| host_id | String | Chave estrangeira para a dimensão anfitrião (FK) |
| id_location | Integer | Chave estrangeira para a dimensão localização (FK) |
| price | Float | Preço da diária do imóvel |
| minimum_nights | Integer | Quantidade mínima de noites para reserva |
| number_of_reviews | Integer | Número total de avaliações recebidas |
| reviews_per_month | Float | Média de avaliações recebidas por mês |
| availability_365 | Integer | Dias disponíveis para reserva nos próximos 365 dias |
| number_of_reviews_ltm | Integer | Número de avaliações nos últimos 12 meses |

### `dim_location`

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| id_location | Integer | Identificador único da localização (PK) |
| neighbourhood | String | Nome do bairro |
| neighbourhood_group | String | Zona ou região dos bairros da cidade |

### `dim_host`

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| host_id | String | Identificador único do anfitrião (PK) |
| host_name | String | Nome do anfitrião |
| host_total_listings_count | Integer | Quantidade total de anúncios que este anfitrião possui |

### `dim_ad`

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| ad_id | String | Identificador de cada anúncio (PK) |
| ad_title | String | Título do anúncio |
| room_type | String | Tipo de acomodação |


---

## 7. Solução do Problema (Insights Obtidos)

Com base nos dados processados, as perguntas feitas inicialmente foram respondidas e análise completa está disponível no [notebook](https://github.com/juliafarah/MVP_Data_Engineering/blob/main/MVP_Pipeline_Airbnb_Rio.ipynb).

As principais conclusões foram:  

* **Zona Sul:** Mantém sua hegemonia em volume e liquidez ao abraçar o turismo de massa.  Copacabana consolida-se como o líder absoluto em número de anúncios, detendo 10.500 anúncios cadastrados na plataforma. Além disso, a demanda transborda para bairros satélites como Gávea, Lagoa e Vidigal, que oferecem custo-benefício ou experiências diferenciadas em comparação aos bairros mais caros e tradicionais da orla.

  
* **Zona Central:** A região central valida a aposta na revitalização (Reviver Centro e Porto Maravilha) e no turismo cultural. Santa Teresa (58%) e Centro (52%) mantêm ocupação competitiva o que a torna uma aposta inteligente tanto para pequenos investidores quanto para empresas especializadas atentos na retomada do turismo cultural e corporativo.

  
* **Zona Oeste:** A busca por imóveis mais modernos e praias mais tranquilas garante as maiores taxas da cidade. A **Barra da Tijuca** possui o maior portfólio (2.739 anúncios) da região acompanhada de uma taxa de ocupação de 56%. A alta liquidez da região indica que é um mercado seguro de giro constante.


---

## 8. Autoavaliação e Conclusão

O pipeline construído atingiu o objetivo de estruturar dados desorganizados em informações estratégicas. A decisão de **modelar os dados em Star Schema (Fato + 3 Dimensões)** desde as etapas iniciais de transformação provou-se eficaz, facilitando a análise e garantindo a escalabilidade do modelo. A utilização do PySpark permitiu a manipulação dos dados através das camadas Bronze, Silver e Gold.

