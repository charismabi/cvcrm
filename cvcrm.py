'''
https://econ.cvcrm.com.br/api/cvio/lead?dataDe=2022-05-05&dataAte=2022-05-06&limit=30
'''

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from datetime import datetime, date, timedelta
import time
from lib import lib, libObj
import psycopg2

class lead_report(libObj.BaseObj):
  id = 0
  create_datetime = ''
  hb_midia_lote_id = ''

  id = 0
  id_report = 0
  situacao = ''
  situacaoanterior = ''
  datahoraprimeirocadastro = ''
  Nome = ''
  email = ''
  telefone = ''
  pontovenda = ''
  empreendimento = ''
  primeiroempreendimento = ''
  ultimoempreendimento = ''
  codigointerno = ''
  precadastros = 0
  reserva = 0
  simulacao = 0
  ganhos = 0
  perdas = 0
  totalvencimentos = 0
  gestorprimeiro = ''
  gestorultimo = ''
  gestor = ''
  corretor = ''
  categoriacorretor = ''
  nivelcorretor = ''
  imobiliariaprimeira = ''
  imobiliariaultima = ''
  imobiliaria = ''
  corretoranterior = ''
  penultimocorretor = ''
  primeiraorigem = ''
  ultimaorigem = ''
  produtolead = ''
  motivocancelamento = ''
  descricaomotivocancelamento = ''
  datacancelamento = ''
  dataprimeirainteracaogestor_sdr = ''
  dataprimeirainteracaocorretor = ''
  dataultimainteracao = ''
  dataenviocorretoranterior = ''
  dataenviocorretor = ''
  diassemcontato = 0
  datareativacao = '' # data do ultimo empreendimento
  leadvencido = ''
  primeiramidia = ''
  ultimamidia = ''
  possibilidade = ''
  primeiraconversao = ''
  ultimaconversao = ''
  emailalternativo = ''
  sexo = ''
  profissao = ''
  datanascimento = ''
  telefonealternativo = ''
  cep = ''
  endereco = ''
  numero = ''
  complemento = ''
  bairro = ''
  estado = ''
  cidade = ''
  documento = ''
  renda = ''
  tag = ''
  tempoconversaoreserva = ''
  tempoconversaovenda = ''
  primeiracampanha = ''
  ultimacampanha = ''
  dataultimaalteracaosituacao = ''
  origemrd = ''
  bolsao = ''
  momentolead = ''

  def datacadastroToTimeStamp(self):
    return lib.dateToSql(self.data)

  def getText(self):
    return \
      'pagina: ' + str(self.pagina) + \
      ' - linha: ' + self.linha + \
      ' - ultimaorigem: ' + self.ultimaorigem + \
      ' - ultimaconversao: ' + self.ultimaconversao + \
      ' - ultimoempreendimento: ' + self.ultimoempreendimento + \
      ' - data_reativacao: ' + self.data_reativacao + \
      ' - create_datetime: ' + self.create_datetime + \
      ' - hb_midia_lote_id: ' + str(self.hb_midia_lote_id)

  def insertPostgres(self, cursor):
    try:
      record_str = self.getText()

      def strv(value):
        return str(value) + ', '

      query = 'INSERT INTO cvcrm.lead_report (id_report, situacao, situacaoanterior, datahoraprimeirocadastro, nome, email, telefone, pontovenda, empreendimento, primeiroempreendimento, ultimoempreendimento, codigointerno, precadastros, reserva, simulacao, ganhos, perdas, totalvencimentos, gestorprimeiro, gestorultimo, gestor, corretor, categoriacorretor, nivelcorretor, imobiliariaprimeira, imobiliariaultima, imobiliaria, corretoranterior, penultimocorretor, primeiraorigem, ultimaorigem, produtolead, motivocancelamento, descricaomotivocancelamento, datacancelamento, dataprimeirainteracaogestor_sdr, dataprimeirainteracaocorretor, dataultimainteracao, dataenviocorretoranterior, dataenviocorretor, diassemcontato, datareativacao, leadvencido, primeiramidia, ultimamidia, possibilidade, primeiraconversao, ultimaconversao, emailalternativo, sexo, profissao, datanascimento, telefonealternativo, cep, endereco, numero, complemento, bairro, estado, cidade, documento, renda, tag, tempoconversaoreserva, tempoconversaovenda, primeiracampanha, ultimacampanha, dataultimaalteracaosituacao, origemrd, bolsao, momentolead, create_datetime, hb_midia_lote_id)' + \
        strv(self.id_report) + \
        strv(self.situacao) + \
        strv(self.situacaoanterior) + \
        strv(self.datahoraprimeirocadastro) + \
        strv(self.nome) + \
        strv(self.email) + \
        strv(self.telefone) + \
        strv(self.pontovenda) + \
        strv(self.empreendimento) + \
        strv(self.primeiroempreendimento) + \
        strv(self.ultimoempreendimento) + \
        strv(self.codigointerno) + \
        strv(self.precadastros) + \
        strv(self.reserva) + \
        strv(self.simulacao) + \
        strv(self.ganhos) + \
        strv(self.perdas) + \
        strv(self.totalvencimentos) + \
        strv(self.gestorprimeiro) + \
        strv(self.gestorultimo) + \
        strv(self.gestor) + \
        strv(self.corretor) + \
        strv(self.categoriacorretor) + \
        strv(self.nivelcorretor) + \
        strv(self.imobiliariaprimeira) + \
        strv(self.imobiliariaultima) + \
        strv(self.imobiliaria) + \
        strv(self.corretoranterior) + \
        strv(self.penultimocorretor) + \
        strv(self.primeiraorigem) + \
        strv(self.ultimaorigem) + \
        strv(self.produtolead) + \
        strv(self.motivocancelamento) + \
        strv(self.descricaomotivocancelamento) + \
        strv(self.datacancelamento) + \
        strv(self.dataprimeirainteracaogestor_sdr) + \
        strv(self.dataprimeirainteracaocorretor) + \
        strv(self.dataultimainteracao) + \
        strv(self.dataenviocorretoranterior) + \
        strv(self.dataenviocorretor) + \
        strv(self.diassemcontato) + \
        strv(self.datareativacao) + \
        strv(self.leadvencido) + \
        strv(self.primeiramidia) + \
        strv(self.ultimamidia) + \
        strv(self.possibilidade) + \
        strv(self.primeiraconversao) + \
        strv(self.ultimaconversao) + \
        strv(self.emailalternativo) + \
        strv(self.sexo) + \
        strv(self.profissao) + \
        strv(self.datanascimento) + \
        strv(self.telefonealternativo) + \
        strv(self.cep) + \
        strv(self.endereco) + \
        strv(self.numero) + \
        strv(self.complemento) + \
        strv(self.bairro) + \
        strv(self.estado) + \
        strv(self.cidade) + \
        strv(self.documento) + \
        strv(self.renda) + \
        strv(self.tag) + \
        strv(self.tempoconversaoreserva) + \
        strv(self.tempoconversaovenda) + \
        strv(self.primeiracampanha) + \
        strv(self.ultimacampanha) + \
        strv(self.dataultimaalteracaosituacao) + \
        strv(self.origemrd) + \
        strv(self.bolsao) + \
        strv(self.momentolead) + \
        strv(self.create_datetime) + \
        str(self.hb_midia_lote_id)

      cursor.execute(query)
      record = cursor.fetchone()
      insert_return = record[0]
      print('[' + insert_return + '] ' + record_str)

      self.libDb.hb_midia_lote.linhas_lidas += 1

      return record[0]

    except (Exception, psycopg2.Error) as error:
        print('--------------------------')
        print("Erro-insert:", record_str)
        print(error)
        print('--------------------------')
        self.hb_midia_lote.log('erro-insert-record_str', record_str)
        self.hb_midia_lote.log('erro-insert-error', str(error))




########################################################################################################################

absObj = libObj.BaseObj(3) #3-econ
print('')
print('*** Processo finalizado normalmente ***')
print('')
