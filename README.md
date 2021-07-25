# ftx_trade_history.py
RAY/USD,RAY/PERPの約定履歴の収集を行う。
# ftx_ray_basis_short_simulation.py
RAYはFTXにおいて、borrowingできないので、RAY/USDはショート不可能な通貨である、このため、RAY/USDは上髭が発生しやすい。このプログラムでは、エントリー段階で、現物ロング、先物ショートを行い、エグジット段階で、前述の上髭をマーケットメイクして拾った場合の収益をシミュレーションする。
# ftx_ray_basis_short_order.py
現物ロング、先物ショートのポジションを指定したレートでエントリー可能な場合に構築するプログラム。今後、エグジットするためのコードも追加する必要がある。
