# Bolsista — Instruções de Trabalho

Este diretório contém os formulários e instruções para as tarefas de avaliação manual das fontes bibliométricas.

## Tarefas por fase

### Fase 1 (agora)

1. **Validar a tabela de correspondência (crosswalk)**
   - Arquivo: `../registry/crosswalk_template.csv`
   - Para cada instituição marcada como `crosswalk_confidence: medium`, verificar:
     - O ROR ID está correto? (conferir em https://ror.org)
     - O OpenAlex Institution ID está correto? (conferir em https://openalex.org)
   - Preencher a coluna `validated_by` com seu nome
   - Reportar ambiguidades ao supervisor

2. **Formulários de governança/sustentabilidade**
   - Pasta: `forms/`
   - Um formulário por fonte
   - Instruções em cada formulário

### Fase 2

3. **Fila de revisão de divergências**
   - Gerada automaticamente pelo motor de convergência
   - Arquivo: `data/processed/review_queue_<run_id>.csv`
   - Verificar manualmente os matches de baixa confiança

4. **Formulários INPI, Derwent, Sucupira**
   - Avaliar cobertura de patentes brasileiras
   - Avaliar dados de programas de pós-graduação

## Como preencher os formulários

Todos os formulários são arquivos YAML. Edite com qualquer editor de texto.
Não altere as chaves (campos) — apenas preencha os valores.
Após preenchido, mova o arquivo para `completed/`.

## Dúvidas

Reportar ao supervisor responsável pelo projeto.
