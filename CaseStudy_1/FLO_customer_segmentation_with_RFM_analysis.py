###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# PROJE GÖREVLERİ
###############################################################
# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama

# Adım 1. flo_data_20K.csv verisini okuyunuz.

# Adım 2. Veri setinde
    # a. İlk 10 gözlem,
    # b. Değişken isimleri,
    # c. Betimsel istatistik,
    # d. Boş değer,
    # e. Değişken tipleri, incelemesi yapınız.

# Adım 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.

# Adım 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.

# Adım 5. Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

# Adım 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.

# Adım 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.

# Adım 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

# GÖREV 2: RFM Metriklerinin Hesaplanması

# GÖREV 3: RF ve RFM Skorlarının Hesaplanması

# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması

# GÖREV 5: Aksiyon zamanı!
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.

# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
# ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
# yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.

# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
# alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor.
# Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv olarak kaydediniz.

# GÖREV 6: Tüm süreci fonksiyonlaştırınız.


###############################################################
# GÖREV 1: Veriyi  Hazırlama ve Anlama (Data Understanding)
###############################################################
# Adım 1. flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.

import pandas as pd
import datetime as dt

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

df = pd.read_csv('CRM_Analytics/datasets/flo_data_20k.csv')

# Adım 2. Veri setinde
# a. İlk 10 gözlem,
df.head(10)
# b. Değişken isimleri,
df.columns
# c. Boyut,
df.shape
# d. Betimsel istatistik,
df.describe().T
# e. Boş değer,
df.isnull().sum()
# f. Değişken tipleri, incelemesi yapınız.
df.info()

# Adım 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Herbir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

# toplam alışveriş sayısı
df['order_num_total'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']
# toplam harcama
df['customer_value_total'] = df['customer_value_total_ever_online'] + df['customer_value_total_ever_offline']

# df['customer_omnichannel'] = df['customer_value_total_ever_online'] + df['customer_value_total_ever_offline']
# total_order_count = df.groupby('master_id').agg({'customer_omnichannel': 'sum'})

# Adım 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
date_columns = df.columns[df.columns.str.contains('date')]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
df[date_columns].dtypes
df.info()

# date_columns = [col for col in df.columns if 'date' in col]

# Adım 5. Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısı ve toplam harcamaların dağılımına bakınız.
df.groupby('order_channel').agg({'master_id': 'count',
                                 'order_num_total': 'sum',
                                 'customer_value_total': 'sum'})

# Adım 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.sort_values('customer_value_total', ascending=False).head(10)

# Adım 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values('order_num_total', ascending=False).head(10)

# Adım 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.
def data_prep(dataframe):
    dataframe['order_num_total'] = dataframe['order_num_total_ever_online'] + dataframe['order_num_total_ever_offline']
    # toplam harcama
    dataframe['customer_value_total'] = dataframe['customer_value_total_ever_online'] + dataframe['customer_value_total_ever_offline']
    date_columns = dataframe.columns[dataframe.columns.str.contains('date')]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return dataframe

flo_data = pd.read_csv('CRM_Analytics/datasets/flo_data_20k.csv')
df = data_prep(flo_data)
df.info()

###############################################################
# GÖREV 2: RFM Metriklerinin Hesaplanması
###############################################################
# Uyarı: recency değerini hesaplamak için analiz tarihini, maksimum tarihten 2 gün sonrası olacak şekilde belirleyebilirsiniz
# (Veri setindeki en son alışverişin yapıldığı tarihten 2 gün sonrasını analiz tarihi olarak belirleyin)

# Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.
# recency: müşteri yeniliği, analiz tarihi - en son alışveriş yaptığı tarih
# frequency: müşterinin alışveriş sıklığı
# monetary: müşterinin şirkete bıraktığı parasal değer

# Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.
analysis_date = df['last_order_date'].max() + pd.Timedelta(days=2)

df.groupby('master_id').agg({'last_order_date': lambda x: (analysis_date - x.max()).days,
                             'order_num_total': lambda x: x.sum(),
                             'customer_value_total': lambda x: x.sum()})

# Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.
rfm = df.groupby('master_id').agg({'last_order_date': lambda x: (analysis_date - x.max()).days,
                             'order_num_total': lambda x: x.sum(),
                             'customer_value_total': lambda x: x.sum()})

# Adım 4: Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.
rfm.columns = ['recency', 'frequency', 'monetary']

###############################################################
# GÖREV 3: RF ve RFM Skorlarının Hesaplanması (Calculating RF and RFM Scores)
###############################################################
# Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
pd.qcut(rfm['recency'],5, labels=[5, 4, 3, 2, 1])
pd.qcut(rfm['frequency'].rank(method='first'),5, labels=[1, 2, 3, 4, 5])
pd.qcut(rfm['monetary'],5, labels=[1, 2, 3, 4, 5])

# Adım 2: Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.
rfm['recency_score'] = pd.qcut(rfm['recency'],5, labels=[5, 4, 3, 2, 1])
rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'),5, labels=[1, 2, 3, 4, 5])
rfm['monetary_score'] = pd.qcut(rfm['monetary'],5, labels=[1, 2, 3, 4, 5])

# Adım 3: recency_score ve frequency_score ve monetary_score'u tek bir değişken olarak ifade ediniz ve RFM_SCORE olarak kaydediniz
rfm['RF_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'] .astype(str)
rfm['RFM_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'] .astype(str) + rfm['monetary_score'].astype(str)
rfm.head()

###############################################################
# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
###############################################################
# Oluşturulan RFM skorların daha açıklanabilir olması için segment tanımlama ve  tanımlanan seg_map yardımı ile RF_SCORE'u segmentlere çevirme

# Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız.
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

# Adım 2: Tanımlanan seg_map yardımı ile skorları segmentlere çeviriniz.
rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

###############################################################
# GÖREV 5: Aksiyon zamanı!
###############################################################
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm.groupby('segment').agg({'recency': ['mean', 'count'],
                            'frequency': ['mean', 'count'],
                            'monetary': ['mean', 'count']})

rfm.reset_index(inplace=True)

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulunuz ve müşteri id'lerini csv ye kaydediniz.

# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Bu müşterilerin sadık  ve
# kadın kategorisinden alışveriş yapan kişiler olması planlandı. Müşterilerin id numaralarını csv dosyasına yeni_marka_hedef_müşteri_id.cvs
# olarak kaydediniz.
df.head()
filtered_customer_ids = df[(df['interested_in_categories_12'].str.contains('KADIN')) & (df['customer_value_total'] > 250)]['master_id']
customer_ids = rfm[(rfm['master_id'].isin(filtered_customer_ids)) & (rfm['segment'].isin(['champions','loyal_customers']))]['master_id']
customer_ids.to_csv('yeni_marka_hedef_müşteri_id.cvs', index=False)

# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşterilerden olan ama uzun süredir
# alışveriş yapmayan ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz.
filtered_customer_ids = df[(df['interested_in_categories_12'].str.contains('ERKEK') | df['interested_in_categories_12'].str.contains('COCUK'))]
customer_ids = rfm[(rfm['master_id'].isin(filtered_customer_ids)) & (rfm['segment'].isin(['new_customers', 'cant_loose', 'about_to_sleep', 'hibernating']))]['master_id']
customer_ids.to_csv('indirim_hedef_müşteri_ids', index=False)

###############################################################
# BONUS - Tüm Sürecin Fonksiyonlaştırılması
###############################################################
def data_prep(dataframe):
    dataframe['order_num_total'] = dataframe['order_num_total_ever_online'] + dataframe['order_num_total_ever_offline']
    # toplam harama
    dataframe['customer_value_total'] = dataframe['customer_value_total_ever_online'] + dataframe['customer_value_total_ever_offline']
    date_columns = dataframe.columns[dataframe.columns.str.contains('date')]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return dataframe

def create_rfm(dataframe):
    analysis_date = dataframe['last_order_date'].max() + pd.Timedelta(days=2)

    rfm = dataframe.groupby('master_id').agg({'last_order_date': lambda x: (analysis_date - x.max()).days,
                                       'order_num_total': lambda x: x.sum(),
                                       'customer_value_total': lambda x: x.sum()})
    rfm.columns = ['recency', 'frequency', 'monetary']

    rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
    rfm['monetary_score'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

    rfm['RF_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str)
    rfm['RFM_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm[
        'monetary_score'].astype(str)
    rfm.head()
    # GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
    return rfm

flo_data = pd.read_csv('CRM_Analytics/datasets/flo_data_20k.csv')
df = data_prep(flo_data)
rfm = create_rfm(df)
rfm.head()

# ------------------
filtered_customer_ids = df[(df['interested_in_categories_12'].str.contains('KADIN')) & (df['customer_value_total'] > 250)]['master_id']
customer_ids = rfm[(rfm['master_id'].isin(filtered_customer_ids)) & (rfm['segment'].isin(['champions','loyal_customers']))]['master_id']
customer_ids.to_csv('yeni_marka_hedef_müşteri_id.cvs', index=False)

# ------------------
filtered_customer_ids = df[(df['interested_in_categories_12'].str.contains('ERKEK') | df['interested_in_categories_12'].str.contains('COCUK'))]
customer_ids = rfm[(rfm['master_id'].isin(filtered_customer_ids)) & (rfm['segment'].isin(['new_customers', 'cant_loose', 'about_to_sleep', 'hibernating']))]['master_id']
customer_ids.to_csv('indirim_hedef_müşteri_ids', index=False)












