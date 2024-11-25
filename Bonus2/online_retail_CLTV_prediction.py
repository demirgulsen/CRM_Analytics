###############################################################
# BG-NBD ve Gamma-Gamma ile CLTV Tahmini
###############################################################
###############################################################
# İş Problemi (Business Problem)
###############################################################
# İngiltere merkezli perakende şirketi satış ve pazarlama faaliyetleri için roadmap belirlemek istemektedir.
# Şirketin orta uzun vadeli plan yapabilmesi için var olan müşterilerin gelecekte şirkete sağlayacakları
# potansiyel değerin tahmin edilmesi gerekmektedir.
################################################################
# Veri Seti Hikayesi
###############################################################
# Online Retail II isimli veri seti İngiltere merkezli bir perakende şirketinin 01/12/2009 - 09/12/2011 tarihleri
# arasındaki online satış işlemlerini içeriyor. Şirketin ürün kataloğunda hediyelik eşyalar yer almaktadır ve çoğu
# müşterisinin toptancı olduğu bilgisi mevcuttur.

###############################################################
# PROJE GÖREVLERİ
###############################################################
#####################################################################################
# GÖREV 1: BG-NBD ve Gamma-Gamma Modellerini Kurarak 6 Aylık CLTV Tahmini Yapılması
#####################################################################################
# DİKKAT!
# Sıfırdan model kurulmasına gerek yoktur. Önceki görevde oluşturulan model üzerinden ilerlenebilir.

# Adım 1: 2010-2011 yıllarındaki veriyi kullanarak İngiltere’deki müşteriler için 6 aylık CLTV tahmini yapınız.
import pandas as pd
import datetime as dt
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter
from lifetimes.plotting import plot_period_transactions

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

retail_df = pd.read_excel('CRM_Analytics/datasets/online_retail_II.xlsx',sheet_name="Year 2010-2011")


def outlier_thresholds(dataframe, variable):
    quartile1 = dataframe[variable].quantile(0.01)
    quartile3 = dataframe[variable].quantile(0.99)
    interquantile_range = quartile3 - quartile1
    up_limit = quartile3 + 1.5 * interquantile_range
    low_limit = quartile1 - 1.5 * interquantile_range
    return low_limit, up_limit


def replace_with_thresholds(dataframe, variable):
    low_limit, up_limit = outlier_thresholds(dataframe, variable)
    # dataframe.loc[(dataframe[variable] < low_limit), variable] = low_limit
    dataframe.loc[(dataframe[variable] > up_limit), variable] = up_limit

def data_prep(dataframe):
    dataframe.dropna(inplace=True)
    dataframe = dataframe[~dataframe['Invoice'].str.contains('C', na=False)]
    dataframe = dataframe[dataframe["Quantity"] > 0]
    dataframe = dataframe[dataframe["Price"] > 0]
    replace_with_thresholds(dataframe, "Quantity")
    replace_with_thresholds(dataframe, "Price")
    dataframe['TotalPrice'] = dataframe['Quantity'] * dataframe['Price']
    dataframe['Customer ID'].astype(int)
    return dataframe


def create_cltv_df(dataframe, month=6):
    analysis_date = dataframe['InvoiceDate'].max() + dt.timedelta(days=2)
    cltv_df = dataframe.groupby('Customer ID').agg({'InvoiceDate': [lambda date: (date.max() - date.min()).days / 7,    # recency
                                                                    lambda date: (analysis_date - date.min()).days / 7],       # tenure
                                                    'Invoice': lambda invoice: invoice.nunique(),                    # frequency
                                                    'TotalPrice': lambda total_price: total_price.sum()})            # monetary

    cltv_df.columns = cltv_df.columns.droplevel(0)
    cltv_df.columns = ['recency', 'T', 'frequency', 'monetary']
    cltv_df.reset_index(inplace=True)

    cltv_df['monetary'] = cltv_df['monetary'] / cltv_df['frequency']  # monetary için satın alma başına ortalama kazancı hesaplamalıyız
    cltv_df = cltv_df[cltv_df['monetary'] > 0]   # her ihtimale karşılık kodda bulunması iyi olur
    cltv_df = cltv_df[cltv_df['frequency'] > 1]

    bgf = BetaGeoFitter(penalizer_coef=0.001)
    bgf.fit(cltv_df['frequency'], cltv_df['recency'], cltv_df['T'])
    # 6 aylık beklenen satın alma sayılarını hesaplayalım
    cltv_df['exp_sales_6_month'] = bgf.conditional_expected_number_of_purchases_up_to_time(4*6,
                                                                                           cltv_df['frequency'],
                                                                                           cltv_df['recency'],
                                                                                           cltv_df['T'])

    ggf = GammaGammaFitter(penalizer_coef=0.01)
    ggf.fit(cltv_df['frequency'], cltv_df['monetary'])
    cltv_df['expected_average_profit'] = ggf.conditional_expected_average_profit(cltv_df['frequency'],
                                                                                 cltv_df['monetary'])

    cltv_df['cltv'] = ggf.customer_lifetime_value(bgf,
                                                  cltv_df['frequency'],
                                                  cltv_df['recency'],
                                                  cltv_df['T'],
                                                  cltv_df['monetary'],
                                                  time=month,
                                                  freq="W",  # T'nin frekans bilgisi. Haftalık
                                                  discount_rate=0.01)

    return cltv_df

df_ = retail_df.copy()
df = data_prep(df_)
df.head()
df.describe().T

cltv_df = create_cltv_df(df)
cltv_df.head()

# Adım 2: Elde ettiğiniz sonuçları yorumlayıp, değerlendiriniz.
cltv_df.sort_values('expected_average_profit', ascending=False).head(20)

# Yorum:
# Beklenen ortalama kazanç değerine (expected_average_profit) göre sıralayıp ilk 20 gözlemi incelediğimizde;
# 1. recency değeri 0 olan müşterilerden 2 tanesinin 6 aylık satış tahmini 0 iken diğer müşterinin 9 tane olduğu görülüyor. Bu da bize hangi müşterilerin risk grubunda olabileceğini anlamamızı sağlıyor
# 2. sık alışveriş yapan kullanıcıların (frequency değeri yüksek olanlar) 6 ay içinde beklenen satış sayılarının daha yüksek olduğu görülmektedir.
# 3. monetary ve 6 aylık expected_average_profit değerlerinin birbirine çok yakın olduğu görülmektedir. Bu da satın alma davranışlarının benzer şekilde devam edeceğini göstermektedir.
# Ayrıca modelin müşetir davranışlarını iyi öğrenmiş ve tahminlerini iyi bir şekilde yansıttığını göstermektedir.
# Bu durum aynı zamanda şirket olarak büyümenin olmadığını da gösterebilir ve bu yönde stratejiler geliştirilmesi gerekir.
# 4. Bu sonuçlardan yola çıkarak hangi müşetirlerin gelecekte düzenli olarak kar getireceği, hangilerinin riskli grupta olduğu ve hangilerinin şirketi terk edebileceği tahmin edilebilir ve buradan yola çıkarak müşterilere özel stratejiler geliştirilebilir

#####################################################################################
# Görev 2: Farklı Zaman Periyotlarından Oluşan CLTV Analizi
#####################################################################################
# Adım 1: 2010-2011 UK müşterileri için 1 aylık ve 12 aylık CLTV hesaplayınız.
uk_df = df[df['Country'] =='United Kingdom']

uk_1_month_cltv = create_cltv_df(uk_df, 1)

uk_12_month_cltv = create_cltv_df(uk_df, 12)

# Adım 2: 1 aylık CLTV'de en yüksek olan 10 kişi ile 12 aylık'taki en yüksek 10 kişiyi analiz ediniz.
uk_1_month_cltv.sort_values('cltv', ascending=False)[:10]
uk_12_month_cltv.sort_values('cltv', ascending=False)[:10]

# Adım 3: Fark var mı? Varsa sizce neden olabilir?

# Her iki sonuç için çok önemli farklar görülmemekle beraber cltv değerlerinin bir miktar artmış olduğunu görüyoruz.
# Yani müşetirler benzer satın alma davranışı göstermeye devam ediyor görünüyor

#####################################################################################
# Görev 3: Segmentasyon ve Aksiyon Önerileri
#####################################################################################
# Adım 1: 2010-2011 UK müşterileri için 6 aylık CLTV'ye göre tüm müşterilerinizi 4 gruba (segmente) ayırınız ve grup isimlerini veri setine ekleyiniz.
uk_6_month_cltv = create_cltv_df(df)
uk_6_month_cltv['cltv_segment'] = pd.qcut(uk_6_month_cltv['cltv'], 4, ['D', 'C', 'B', 'A'])

# Adım 2: 4 grup içerisinden seçeceğiniz 2 grup için yönetime kısa kısa 6 aylık aksiyon önerilerinde bulununuz.
c
# B grubu için; sık alışveriş yapmalarına rağmen daha az miktarda pararsal değer bırakacağı tahmin edilmekte, bu göz önüne alındığında onlara yönelik daha önce almış olduğu ürünlere benzer ürünler hakkında indirimler düzenlenebilir, sms veya e-posta yoluyla daha fazla indirim imkanları sunulabilir, belirli miktar alışverşlerinde puan kazanma gibi öneriler sunulabilir
# D grubu için; müşterilere kendilerini daha özel hissetmelerini sağlayacak daha samimi teklifler sunulabilir. Alışveriş alışkanlıkları dikkate alınarak onlara özel kampanyalar düzenlenebilir






