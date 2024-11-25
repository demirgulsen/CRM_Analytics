##############################################################
# BG-NBD ve Gamma-Gamma ile CLTV Prediction
##############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO satış ve pazarlama faaliyetleri için roadmap belirlemek istemektedir.
# Şirketin orta uzun vadeli plan yapabilmesi için var olan müşterilerin gelecekte
# şirkete sağlayacakları potansiyel değerin tahmin edilmesi gerekmektedir.

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
# GÖREVLER
###############################################################
# GÖREV 1: Veriyi Hazırlama
           # 1. flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.
           # 2. Aykırı değerleri baskılamak için gerekli olan outlier_thresholds ve replace_with_thresholds fonksiyonlarını tanımlayınız.
           # Not: cltv hesaplanırken frequency değerleri integer olması gerekmektedir.Bu nedenle alt ve üst limitlerini round() ile yuvarlayınız.
           # 3. "order_num_total_ever_online","order_num_total_ever_offline","customer_value_total_ever_offline","customer_value_total_ever_online" değişkenlerinin
           # aykırı değerleri varsa baskılayanız.
           # 4. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
           # alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
           # 5. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.

# GÖREV 2: CLTV Veri Yapısının Oluşturulması
           # 1.Veri setindeki en son alışverişin yapıldığı tarihten 2 gün sonrasını analiz tarihi olarak alınız.
           # 2.customer_id, recency_cltv_weekly, T_weekly, frequency ve monetary_cltv_avg değerlerinin yer aldığı yeni bir cltv dataframe'i oluşturunuz.
           # Monetary değeri satın alma başına ortalama değer olarak, recency ve tenure değerleri ise haftalık cinsten ifade edilecek.


# GÖREV 3: BG/NBD, Gamma-Gamma Modellerinin Kurulması, CLTV'nin hesaplanması
           # 1. BG/NBD modelini fit ediniz.
                # a. 3 ay içerisinde müşterilerden beklenen satın almaları tahmin ediniz ve exp_sales_3_month olarak cltv dataframe'ine ekleyiniz.
                # b. 6 ay içerisinde müşterilerden beklenen satın almaları tahmin ediniz ve exp_sales_6_month olarak cltv dataframe'ine ekleyiniz.
           # 2. Gamma-Gamma modelini fit ediniz. Müşterilerin ortalama bırakacakları değeri tahminleyip exp_average_value olarak cltv dataframe'ine ekleyiniz.
           # 3. 6 aylık CLTV hesaplayınız ve cltv ismiyle dataframe'e ekleyiniz.
                # a. Hesapladığınız cltv değerlerini standarlaştırıp scaled_cltv değişkeni oluşturunuz.
                # b. Cltv değeri en yüksek 20 kişiyi gözlemleyiniz.

# GÖREV 4: CLTV'ye Göre Segmentlerin Oluşturulması
           # 1. 6 aylık standartlaştırılmış CLTV'ye göre tüm müşterilerinizi 4 gruba (segmente) ayırınız ve grup isimlerini veri setine ekleyiniz. cltv_segment ismi ile dataframe'e ekleyiniz.
           # 2. 4 grup içerisinden seçeceğiniz 2 grup için yönetime kısa kısa 6 aylık aksiyon önerilerinde bulununuz

# GÖREV 5: Tüm süreci fonksiyonlaştırınız.


###############################################################
# GÖREV 1: Veriyi Hazırlama
###############################################################
# 1. flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.
import pandas as pd
import datetime as dt
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter
from sklearn.preprocessing import MinMaxScaler

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
# pd.options.mode.chained_assignment = None

flo_data = pd.read_csv('CRM_Analytics/datasets/flo_data_20k.csv')
df = flo_data.copy()

# 2. Aykırı değerleri baskılamak için gerekli olan outlier_thresholds ve replace_with_thresholds fonksiyonlarını tanımlayınız.
# Not: cltv hesaplanırken frequency değerleri integer olması gerekmektedir.Bu nedenle alt ve üst limitlerini round() ile yuvarlayınız.
def outlier_thresholds(dataframe, variable):
    quartile1 = dataframe[variable].quantile(0.01)
    quartile3 = dataframe[variable].quantile(0.99)
    interquantile_range = quartile3 - quartile1
    up_limit = quartile3 + 1.5 * interquantile_range
    low_limit = quartile1 - 1.5 * interquantile_range
    return low_limit, up_limit

def replace_with_thresholds(dataframe, variable):
    low_limit, up_limit = outlier_thresholds(dataframe, variable)
    # dataframe.loc[(dataframe[variable] < low_limit), variable] = round(low_limit, 0)
    dataframe.loc[(dataframe[variable] > up_limit), variable] = round(up_limit, 0)

# 3. "order_num_total_ever_online","order_num_total_ever_offline","customer_value_total_ever_offline","customer_value_total_ever_online" değişkenlerinin
# aykırı değerleri varsa baskılayanız.
columns = ["order_num_total_ever_online","order_num_total_ever_offline","customer_value_total_ever_offline","customer_value_total_ever_online"]
for col in columns:
    replace_with_thresholds(df, col)

# 4. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
df['order_num_total'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']

df['customer_value_total'] = df['customer_value_total_ever_online'] + df['customer_value_total_ever_offline']

# 5. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.info()

date_columns = df.columns[df.columns.str.contains('date')]
df[date_columns] = df[date_columns].apply(pd.to_datetime)

###############################################################
# GÖREV 2: CLTV Veri Yapısının Oluşturulması
###############################################################
# 1.Veri setindeki en son alışverişin yapıldığı tarihten 2 gün sonrasını analiz tarihi olarak alınız.
analysis_date = df['last_order_date'].max() + dt.timedelta(days=2)

# 2.customer_id, recency_cltv_weekly, T_weekly, frequency ve monetary_cltv_avg değerlerinin yer aldığı yeni bir cltv dataframe'i oluşturunuz.
# Monetary değeri satın alma başına ortalama değer olarak, recency ve tenure değerleri ise haftalık cinsten ifade edilecek.

# recency: son alışveriş tarihi - ilk alışveriş tarihi
# frequency: alışveriş sıklığı
# tenure: analiz tarihi - ilk alışveriş tarihi --> Müşterinin yaşı
# monetary: satın alma başına ortalama parasal değer
cltv_df = pd.DataFrame()
cltv_df['customer_id'] = df['master_id']
cltv_df['recency_cltv_weekly'] = (df['last_order_date'] - df['first_order_date']).dt.days / 7
cltv_df['frequency'] = df['order_num_total'].astype(int)
cltv_df['T_weekly'] = (analysis_date - df['first_order_date']).dt.days / 7
cltv_df['monetary_cltv_avg'] = df['customer_value_total'] / df['order_num_total']

cltv_df.head()
###############################################################
# GÖREV 3: BG/NBD, Gamma-Gamma Modellerinin Kurulması, 6 aylık CLTV'nin hesaplanması
###############################################################
# 1. BG/NBD modelini fit ediniz.
bgf = BetaGeoFitter(penalizer_coef=0.001)
bgf.fit(cltv_df['frequency'],
        cltv_df['recency_cltv_weekly'],
        cltv_df['T_weekly'])


# a. 3 ay içerisinde müşterilerden beklenen satın almaları tahmin ediniz ve exp_sales_3_month olarak cltv dataframe'ine ekleyiniz.
cltv_df['exp_sales_3_month'] = bgf.conditional_expected_number_of_purchases_up_to_time(4*3,
                                                                                       cltv_df['frequency'],
                                                                                       cltv_df['recency_cltv_weekly'],
                                                                                       cltv_df['T_weekly'])

# 2. Yöntem
# cltv_df["exp_sales_3_month"] = bgf.predict(4*3,
#                                        cltv_df['frequency'],
#                                        cltv_df['recency_cltv_weekly'],
#                                        cltv_df['T_weekly'])

# b. 6 ay içerisinde müşterilerden beklenen satın almaları tahmin ediniz ve exp_sales_6_month olarak cltv dataframe'ine ekleyiniz.
cltv_df['exp_sales_6_month'] = bgf.conditional_expected_number_of_purchases_up_to_time(4*6,
                                                                                       cltv_df['frequency'],
                                                                                       cltv_df['recency_cltv_weekly'],
                                                                                       cltv_df['T_weekly'])


# 2. Gamma-Gamma modelini fit ediniz. Müşterilerin ortalama bırakacakları değeri tahminleyip exp_average_value olarak cltv dataframe'ine ekleyiniz.
# --> average prifit' i  modelleme

ggf = GammaGammaFitter(penalizer_coef=0.01)

ggf.fit(cltv_df['frequency'], cltv_df['monetary_cltv_avg'])

cltv_df['exp_average_value'] = ggf.conditional_expected_average_profit(cltv_df['frequency'],
                                                                       cltv_df['monetary_cltv_avg'])

# 3. 6 aylık CLTV hesaplayınız ve cltv ismiyle dataframe'e ekleyiniz.
cltv_df['cltv'] = ggf.customer_lifetime_value(bgf,
                                              cltv_df['frequency'],
                                              cltv_df['recency_cltv_weekly'],
                                              cltv_df['T_weekly'],
                                              cltv_df['monetary_cltv_avg'],
                                              time=6,
                                              freq='W',
                                              discount_rate=0.01)

# a. Hesapladığınız cltv değerlerini standarlaştırıp scaled_cltv değişkeni oluşturunuz.
from sklearn.preprocessing import StandardScaler

# Standartlaştırma işlemi
scaler = StandardScaler()
cltv_df['standardized_cltv'] = scaler.fit_transform(cltv_df[['cltv']])

# b. Cltv değeri en yüksek 20 kişiyi gözlemleyiniz.
cltv_df.sort_values('cltv', ascending=False)[:20]

###############################################################
# GÖREV 4: CLTV'ye Göre Segmentlerin Oluşturulması
###############################################################
# 1. 6 aylık standartlaştırılmış CLTV'ye göre tüm müşterilerinizi 4 gruba (segmente) ayırınız ve grup isimlerini veri setine ekleyiniz. cltv_segment ismi ile dataframe'e ekleyiniz.
cltv_df['cltv_segment'] = pd.qcut(cltv_df['cltv'], 4, ['D', 'C', 'B', 'A'] )

cltv_df.head()

# 2. 4 grup içerisinden seçeceğiniz 2 grup için yönetime kısa kısa 6 aylık aksiyon önerilerinde bulununuz

###############################################################
# BONUS: Tüm süreci fonksiyonlaştırınız.
###############################################################
def outlier_thresholds(dataframe, variable):
    quartile1 = dataframe[variable].quantile(0.01)
    quartile3 = dataframe[variable].quantile(0.99)
    interquantile_range = quartile3 - quartile1
    up_limit = quartile3 + 1.5 * interquantile_range
    low_limit = quartile1 - 1.5 * interquantile_range
    return low_limit, up_limit

def replace_with_thresholds(dataframe, variable):
    low_limit, up_limit = outlier_thresholds(dataframe, variable)
    # dataframe.loc[(dataframe[variable] < low_limit), variable] = round(low_limit, 0)
    dataframe.loc[(dataframe[variable] > up_limit), variable] = round(up_limit, 0)

def create_cltv_df(dataframe):
    columns = ["order_num_total_ever_online", "order_num_total_ever_offline", "customer_value_total_ever_offline",
               "customer_value_total_ever_online"]
    for col in columns:
        replace_with_thresholds(df, col)

    df['order_num_total'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']
    df['customer_value_total'] = df['customer_value_total_ever_online'] + df['customer_value_total_ever_offline']

    date_columns = df.columns[df.columns.str.contains('date')]
    df[date_columns] = df[date_columns].apply(pd.to_datetime)

    analysis_date = df['last_order_date'].max() + dt.timedelta(days=2)
    cltv_df = pd.DataFrame()
    cltv_df['customer_id'] = df['master_id']
    cltv_df['recency_cltv_weekly'] = (df['last_order_date'] - df['first_order_date']).dt.days / 7
    cltv_df['frequency'] = df['order_num_total'].astype(int)
    cltv_df['T_weekly'] = (analysis_date - df['first_order_date']).dt.days / 7
    cltv_df['monetary_cltv_avg'] = df['customer_value_total'] / df['order_num_total']

    bgf = BetaGeoFitter(penalizer_coef=0.001)
    bgf.fit(cltv_df['frequency'],
            cltv_df['recency_cltv_weekly'],
            cltv_df['T_weekly'])

    cltv_df['exp_sales_3_month'] = bgf.conditional_expected_number_of_purchases_up_to_time(4*3,
                                                                                           cltv_df['frequency'],
                                                                                           cltv_df['recency_cltv_weekly'],
                                                                                           cltv_df['T_weekly'])

    cltv_df['exp_sales_6_month'] = bgf.conditional_expected_number_of_purchases_up_to_time(4*6,
                                                                                           cltv_df['frequency'],
                                                                                           cltv_df['recency_cltv_weekly'],
                                                                                           cltv_df['T_weekly'])

    ggf = GammaGammaFitter(penalizer_coef=0.01)
    ggf.fit(cltv_df['frequency'], cltv_df['monetary_cltv_avg'])
    cltv_df['exp_average_value'] = ggf.conditional_expected_average_profit(cltv_df['frequency'],
                                                                           cltv_df['monetary_cltv_avg'])

    cltv_df['cltv'] = ggf.customer_lifetime_value(bgf,
                                                  cltv_df['frequency'],
                                                  cltv_df['recency_cltv_weekly'],
                                                  cltv_df['T_weekly'],
                                                  cltv_df['monetary_cltv_avg'],
                                                  time=6,
                                                  freq='W',
                                                  discount_rate=0.01)

    cltv_df['cltv_segment'] = pd.qcut(cltv_df['cltv'], 4, ['D', 'C', 'B', 'A'])

    return cltv_df


df = flo_data.copy()
cltv_df = create_cltv_df(df)
cltv_df.head()


filtered_customer_ids = df[(df['interested_in_categories_12'].str.contains('ERKEK') | df['interested_in_categories_12'].str.contains('COCUK'))]
filtered_customer_ids.rename(columns={'master_id': 'customer_id'}, inplace=True)
filtered_customer_ids = filtered_customer_ids['customer_id']
customer_ids = cltv_df[(cltv_df['customer_id'].isin(filtered_customer_ids)) & (cltv_df['cltv_segment'].isin(['A', 'B']))]
filtered_customer_ids