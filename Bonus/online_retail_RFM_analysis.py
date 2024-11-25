###############################################################
# RFM Analizi İle Müşteri Segmentayonu
###############################################################
###############################################################
# İş Problemi (Business Problem)
###############################################################
# İngiltere merkezli perakende şirketi müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istemektedir.
# Ortak davranışlar sergileyen müşteri segmentleri özelinde pazarlama çalışmaları yapmanın gelir artışı sağlayacağını düşünmektedir.
# Segmentlere ayırmak için RFM analizi kullanılacaktır.

################################################################
# Veri Seti Hikayesi
###############################################################
# Online Retail II isimli veri seti İngiltere merkezli bir perakende şirketinin 01/12/2009 - 09/12/2011 tarihleri
# arasındaki online satış işlemlerini içeriyor. Şirketin ürün kataloğunda hediyelik eşyalar yer almaktadır ve çoğu
# müşterisinin toptancı olduğu bilgisi mevcuttur.

###############################################################
# PROJE GÖREVLERİ
###############################################################
# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
import pandas as pd
import datetime as dt

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# Adım 1: Online Retail II excelindeki 2010-2011 verisini okuyunuz. Oluşturduğunuz dataframe’in kopyasını oluşturunuz.
retail_df = pd.read_excel('CRM_Analytics/datasets/online_retail_II.xlsx',sheet_name="Year 2010-2011")
df = retail_df.copy()
df.head()
df.info()


# Adım 2: Veri setinin betimsel istatistiklerini inceleyiniz.
df.describe().T

# Adım 3: Veri setinde eksik gözlem var mı? Varsa hangi değişkende kaç tane eksik gözlem vardır?
df.isnull().sum()

# Adım 4: Eksik gözlemleri veri setinden çıkartınız. Çıkarma işleminde ‘inplace=True’ parametresini kullanınız.
df.dropna(inplace=True)
df.isnull().sum()

# Adım 5: Eşsiz ürün sayısı kaçtır?
for col in df.columns:
    print(col, "-->", df[col].nunique())
    print("############################")

# Adım 6: Hangi üründen kaçar tane vardır?
for col in df.columns:
    print(df[col].value_counts())
    print("############################")

# Adım 7: En çok sipariş edilen 5 ürünü çoktan aza doğru sıralayınız
df['Description'].value_counts()[:5]

# Adım 8: Faturalardaki ‘C’ iptal edilen işlemleri göstermektedir. İptal edilen işlemleri veri setinden çıkartınız.
df = df[~df['Invoice'].str.contains('C', na=False)]

# Adım 9: Fatura başına elde edilen toplam kazancı ifade eden ‘TotalPrice’ adında bir değişken oluşturunuz
df['TotalPrice'] = df['Quantity'] * df['Price']

###############################################################
# GÖREV 2: RFM Metriklerinin Hesaplanması
###############################################################
# DİKKAT!
# recency değeri için bugünün tarihini (2011, 12, 11) olarak kabul ediniz.
# rfm dataframe’ini oluşturduktan sonra veri setini "monetary>0" olacak şekilde filtreleyiniz.

# Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.
# recency: müşteri yeniliği --> analiz tarihi - en son alışveriş tarihi
# frequency: alışveriş sıklığı
# monetary: Müşterini şirkete bıraktığı toplam parasal değer

# Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini groupby, agg ve lambda ile hesaplayınız.
analysis_date = df['InvoiceDate'].max() + dt.timedelta(days=2)

df.groupby('Customer ID').agg({'InvoiceDate': lambda x: (analysis_date - x.max()).days,    #recency
                               'Invoice': lambda x: x.nunique(),      # frequency
                               'TotalPrice': lambda x: x.sum()})      # monetary

# Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.
rfm = df.groupby('Customer ID').agg({'InvoiceDate': lambda x: (analysis_date - x.max()).days,    #recency
                               'Invoice': lambda x: x.nunique(),      # frequency
                               'TotalPrice': lambda x: x.sum()})      # monetary

# Adım 4: Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.
rfm.columns = ['recency', 'frequency', 'monetary']

rfm = rfm[rfm['monetary'] > 0]
###############################################################
# Görev 3: RFM Skorlarının Oluşturulması ve Tek bir Değişkene Çevrilmesi
###############################################################
# Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
# ve bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.
rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=['5', '4', '3', '2', '1'])
rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=['1', '2', '3', '4', '5'], duplicates='drop')
rfm['monetary_score'] = pd.qcut(rfm['monetary'], 5, labels=['1', '2', '3', '4', '5'])

# Adım 2: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.
rfm['RF_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str)

###############################################################
# Görev 4: RF Skorunun Segment Olarak Tanımlanması
###############################################################
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

# Adım 2: seg_map yardımı ile skorları segmentlere çeviriniz.
rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

###############################################################
# Görev 5: Aksiyon Zamanı !
###############################################################
# Adım1: Önemli gördüğünüz 3 segmenti seçiniz. Bu üç segmenti hem aksiyon kararları açısından hem de segmentlerin yapısı açısından(ortalama
# RFM değerleri) yorumlayınız.
rfm[['monetary', 'RF_SCORE', 'segment']].sort_values('monetary', ascending=False)[:30]

# Yorum: İlk 30 gözlem incelendiğinde sadık, potansiyel sadık, uykuda ve riskli sınıfında olanlar müşteriler dikkat çekmektedir.
# Çünkü bu sınıftaki müşteriler monetary değeri yüksek olan yani şirkete yüksek değerde parasal değer bırakan müşterilerdir.
# O yüzden ilk başta bu sınıfataki müşetirilere odaklanamak gerekmektedir. Bu müşterilerinden

# 'sadık' müşterilere hediye çeki, kuponlar, doğum günleri gibi özel günler kişiye özel teklifler, ücretsiz kargo gibi olanaklar, indirimlerden ilk haberdar olma gibi imkanlar sunulabilir,

# 'potansiyel sadık' müşterilere, daha önceki alışverişleri göz önünde bulundurularak kişiselleştirilmiş ürün önerileri, sms ve e-posta ile pazarlama, ortalamalarından daha fazla alışveriş yaptıklarında özel indirim fırsatları gibi imkanlar sunulabilir,

# 'uykuda' sınıfında olan müşteriler için, 'Sizi Özledik' gibi daha samimi mesajlarla onlara kendilerini değerli hissetirecek mesajlarla indirim kuponları gönderilebilir,
# Ayrıca anketler göndererek, neden alışveriş yapmadıklarına dair geri bildirimleri göz önüne alınabilir,

# 'riskli' sınıfındaki müşterilere ise onların ilgisini çekecek özel teklifler sunulabilir, kargo olanakları iyileştirilebilir.
# Daha önceki alışveriş deneyimleri analiz edilerek onalara özel strajiler geliştirilebilişr.
# Örneğin daha önce aldıkları ürüne göre aynı veya benzer ya da tamamlayıcı ürünlerde indirimler uygulanabilir, kargolar ücretsiz sunulabilir

# ********************************************

# NOT: Tüm bunlar şirkete daha fazla para kazandırmak için geliştirilebilecek stratejilerdir. Daha ayrıntılı incelemelerle farklı yöntemler de denenebilir bu işin bir boyutu.
# Bir diğer açıdan baktığımızda yani tüketici olduğumuzu düşündüğümüzde bu tarz kampanyalar her ne kadar hoş görünse de aslında çok da hoşumuza gitmeyebilir. Çünkü
# bizler sadece ihtiyacımız olduğunda alışveriş yapmak isteyebiliriz. Fazla  ve gereksiz ürünler almak istemeyiz,
# özellikle ülkemizdeki ekonomik koşullar göz önüne alındığında bu daha da önemli hale gelmektedr.
# Bir diğer konuda tabi ki kapitalizme ayak uydurmak oluyor :)
# O yüzden bu kampanyaları tüketici gözünden yapmak istediğimizde belki daha güvenilir, dürüst ve sadık bir ilişki ortaya çıkabilir.
# Dürüst ve açık bir yaklaşım sergilemek güvenirliliği artırır. Tüm bunlar göz önüne alındığında bir strateji yapacak olursak;
# kullanıcılardan belli aralıklarla anket, uygulamada buna özel bir alan ile vs.ileride ihtiyacı olabilecek ürünler istenebilir
# ve indirim olduğu takdirde haber verilebilir. Kargo ücretlerinde indirime gidilerek ve müşteri servisleri,
# iade gibi konularda kolaylıklar sağlanarak bu müşterilerin devamlı müşteri olması sağlanabilir ki ben de böyle olmasını isterim.

# ********************************************

# Adım2: "Loyal Customers" sınıfına ait customer ID'leri seçerek excel çıktısını alınız.
rfm.reset_index(inplace=True)
loyal_customer_ids = rfm[rfm['segment'] == 'loyal_customers']['Customer ID']
loyal_customer_ids.to_csv('loyal_customers.csv')
