aws_region         = "sa-east-1"
aws_profile        = "tf_etl_jooaodl"
bucket_name        = "bucket-openaq-etl-jooaodl02"
openaq_secret_name = "openaq/api_key"

# Todas as 52 estacoes do Brasil identificadas em descobrindo_id_brasil/.
# [parada] = sem dado novo desde antes de set/2026; a API responde,
# mas a janela vem vazia e a Lambda apenas pula a estacao.
openaq_location_ids = [
  "589967",  # Sao Paulo [parada]
  "820321",  # Bangu
  "820322",  # Campo Grande
  "820323",  # Centro
  "820324",  # Copacabana
  "820326",  # Irajá
  "820328",  # São Cristóvão [parada]
  "820329",  # Tijuca
  "3047759", # Caxias do Sul [parada]
  "3107138", # teste1 [parada]
  "3852932", # GUAMÁ [parada]
  "3911519", # Parque Vida Nova, Caji
  "5040030", # Manaus
  "5110472", # Campinho
  "5110473", # Engenheiro Leal
  "5110474", # Presidente Vargas
  "5110511", # Campo de Santana
  "5110512", # Quintino
  "5110513", # Cascadura
  "5110514", # Madureira
  "5616182", # Porto Velho - Brux
  "5616694", # Lábrea Caititu
  "5636852", # MAM
  "5636888", # Praça XV
  "5648811", # Parque Madureira
  "6102319", # Imperatriz
  "6126992", # Quality01 [parada]
  "6126993", # Quality02 [parada]
  "6139516", # São Paulo
  "6152580", # Abolição [parada]
  "6152581", # Barra da Tijuca
  "6152582", # Caju [parada]
  "6152583", # Campo Grande Aurassure
  "6152584", # Guaratiba
  "6152585", # Ilha do Governador [parada]
  "6152586", # Irajá Aurassure
  "6152587", # Lagoa [parada]
  "6152588", # Pavuna
  "6152590", # Penha [parada]
  "6152591", # Ramos
  "6152592", # Realengo
  "6152593", # Recreio dos Bandeirantes [parada]
  "6152594", # Rocha [parada]
  "6152595", # Rocinha
  "6152598", # Tanque
  "6178275", # Tijuca Aurassure
  "6199572", # Jacarepaguá
  "6285443", # Santa Cruz 1
  "6285444", # Santa Cruz 2
  "6404277", # Ilha do Governador [parada]
  "6418495", # Recreio dos Bandeirantes
  "6460996", # Sao Paulo
]
