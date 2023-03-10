#%%
from initailFunctionsPath import *
#%%

# Loading the valid symbols
valid_symbols_df = (
    spark.read.parquet(VALID_SYMBOLS_PATH + "/{}".format("Symbols_14001116.parquet"))
    .select("Ticker")
    .withColumnRenamed("Ticker", "symbol")
    .dropDuplicates()
)

# replacing Arabic characters with Persian ones
valid_symbols_df = replace_arabic_characters_and_correct_symbol_names(valid_symbols_df)

display_df(valid_symbols_df)
#%%

# Adding ETFs to valid symbols
ETFs = [
    "آتیمس",
    "آرمانی",
    "آساس",
    "آسام",
    "آسامید",
    "آوا",
    "آکورد",
    "آگاس",
    "ارزش",
    "اطلس",
    "اعتماد",
    "افران",
    "افق ملت",
    "الماس",
    "امین یکم",
    "انار",
    "اهرم",
    "اوج",
    "اوصتا",
    "بذر",
    "تاراز",
    "تصمیم",
    "ثبات",
    "ثروتم",
    "ثمین",
    "ثهام",
    "خاتم",
    "دارا",
    "دارا یکم",
    "داریوش",
    "داریک",
    "رماس",
    "رویش",
    "زر",
    "زرین",
    "زیتون",
    "سبز",
    "سحرخیز",
    "سخند",
    "سرو",
    "سپاس",
    "سپر",
    "سپیدما",
    "سیناد",
    "صایند",
    "صغرب",
    "صنم",
    "صنوین",
    "طلا",
    "عیار",
    "فراز",
    "فردا",
    "فیروزا",
    "مانی",
    "مثقال",
    "مدیر",
    "نارون",
    "نسیم",
    "نهال",
    "هامرز",
    "همای",
    "وبازار",
    "ویستا",
    "پادا",
    "پارند",
    "پالایش",
    "کارا",
    "کاردان",
    "کاریس",
    "کارین",
    "کامیاب",
    "کمند",
    "کهربا",
    "کیان",
    "گنبد",
    "گنجین",
    "گنجینه",
    "گوهر",
    "یارا",
    "یاقوت",
    "فیروزه",
]
ETFs = [(i,) for i in ETFs]
ETFs = spark.createDataFrame(data = ETFs, schema = valid_symbols_df.schema)
valid_symbols_df = valid_symbols_df.union(ETFs).dropDuplicates()
display_df(valid_symbols_df)
#%%

# Adding right offers to valid symbols
right_offers = [
    "آ س پح",
    "آرمانح",
    "آریانح",
    "آرینح",
    "آکنتورح",
    "اتکامح",
    "اتکایح",
    "اخابرح",
    "ارفعح",
    "اعتلاح",
    "افراح",
    "افقح",
    "البرزح",
    "امیدح",
    "امینح",
    "اوانح",
    "بالبرح",
    "بایکاح",
    "بترانسح",
    "بتکح",
    "بدکوح",
    "برکتح",
    "بزاگرسح",
    "بساماح",
    "بسویچح",
    "بشهابح",
    "بصباح",
    "بفجرح",
    "بموتوح",
    "بمیلاح",
    "بنوح",
    "بنیروح",
    "بهپاکح",
    "بپاسح",
    "بکابح",
    "بکامح",
    "بکهنوجح",
    "تاصیکوح",
    "تاپکیشح",
    "تاپیکوح",
    "تایراح",
    "تجلیح",
    "تشتادح",
    "تفیروح",
    "تلیسهح",
    "تماوندح",
    "تمحرکهح",
    "تملتح",
    "تنوینح",
    "توریلح",
    "تپمپیح",
    "تپکوح",
    "تکشاح",
    "تکمباح",
    "تکنارح",
    "تکنوح",
    "تیپیکوح",
    "ثابادح",
    "ثاختح",
    "ثاصفاح",
    "ثالوندح",
    "ثامانح",
    "ثاژنح",
    "ثباغح",
    "ثترانح",
    "ثرودح",
    "ثشاهدح",
    "ثشرقح",
    "ثعتماح",
    "ثعمراح",
    "ثغربح",
    "ثفارسح",
    "ثقزویح",
    "ثمسکنح",
    "ثنورح",
    "ثنوساح",
    "ثپردیسح",
    "جمح",
    "جهرمح",
    "حبندرح",
    "حتایدح",
    "حتوکاح",
    "حخزرح",
    "حسیناح",
    "حفاریح",
    "حپارساح",
    "حپتروح",
    "حکشتیح",
    "حکمتح",
    "خاذینح",
    "خاهنح",
    "خاورح",
    "خبهمنح",
    "ختراکح",
    "ختورح",
    "ختوقاح",
    "خدیزلح",
    "خریختح",
    "خرینگح",
    "خزامیاح",
    "خزرح",
    "خساپاح",
    "خشرقح",
    "خصدراح",
    "خفناورح",
    "خفنرح",
    "خفولاح",
    "خلنتح",
    "خمحرکهح",
    "خمحورح",
    "خمهرح",
    "خموتورح",
    "خنصیرح",
    "خودروح",
    "خوسازح",
    "خپارسح",
    "خپویشح",
    "خچرخشح",
    "خکارح",
    "خکاوهح",
    "خکمکح",
    "خگسترح",
    "دابورح",
    "دارابح",
    "داروح",
    "داسوهح",
    "دالبرح",
    "دامینح",
    "داناح",
    "دبالکح",
    "دتمادح",
    "دتهرانح",
    "دتوزیعح",
    "دتولیح",
    "دتولیدح",
    "دجابرح",
    "ددامح",
    "درازکح",
    "درهآورح",
    "دروزح",
    "دزهراویح",
    "دسانکوح",
    "دسبحاح",
    "دسبحانح",
    "دسیناح",
    "دشیریح",
    "دشیمیح",
    "دعبیدح",
    "دفاراح",
    "دفراح",
    "دقاضیح",
    "دلرح",
    "دلقماح",
    "دهدشتح",
    "دپارسح",
    "دکوثرح",
    "دکپسولح",
    "دکیمیح",
    "دیرانح",
    "رانفورح",
    "رتاپح",
    "رتکوح",
    "رمپناح",
    "رنیکح",
    "رپارسح",
    "رکیشح",
    "زفکاح",
    "زقیامح",
    "زملاردح",
    "زمگساح",
    "زنجانح",
    "زنگانح",
    "زگلدشتح",
    "ساذریح",
    "سارابح",
    "ساربیلح",
    "ساروجح",
    "سارومح",
    "سامانح",
    "سباقرح",
    "سبجنوح",
    "سبحانح",
    "سبهانح",
    "سترانح",
    "سجامح",
    "سخاشح",
    "سخزرح",
    "سخوافح",
    "سخوزح",
    "سدبیرح",
    "سدشتح",
    "سدورح",
    "سرودح",
    "سرچشمهح",
    "سشرقح",
    "سشمالح",
    "سصفهاح",
    "سصوفیح",
    "سغربح",
    "سفارح",
    "سفارسح",
    "سفارودح",
    "سفاسیتح",
    "سفانوح",
    "سقاینح",
    "سلارح",
    "سمازنح",
    "سمایهح",
    "سمگاح",
    "سنوینح",
    "سنیرح",
    "سهرمزح",
    "سهگمتح",
    "سپاهاح",
    "سپح",
    "سپرمیح",
    "سکارونح",
    "سکردح",
    "سکرماح",
    "سیدکوح",
    "سیلامح",
    "شاراکح",
    "شاملاح",
    "شبریزح",
    "شبهرنح",
    "شتهرانح",
    "شتولیح",
    "شتوکاح",
    "شجمح",
    "شخارکح",
    "شدوصح",
    "شرانلح",
    "شرنگیح",
    "شزنگح",
    "شسمح",
    "شسیناح",
    "شصدفح",
    "شصفهاح",
    "شفاراح",
    "شفارسح",
    "شفنح",
    "شلردح",
    "شلعابح",
    "شموادح",
    "شنفتح",
    "شپارسح",
    "شپاسح",
    "شپاکساح",
    "شپتروح",
    "شپدیسح",
    "شپلیح",
    "شپناح",
    "شکبیرح",
    "شکربنح",
    "شکفح",
    "شکلرح",
    "شگلح",
    "شیرازح",
    "شیرانح",
    "صباح",
    "غاذرح",
    "غالبرح",
    "غبشهرح",
    "غبهنوشح",
    "غبهپاکح",
    "غدامح",
    "غدشتح",
    "غسالمح",
    "غشاذرح",
    "غشانح",
    "غشصفاح",
    "غشهدابح",
    "غشهدح",
    "غشوکوح",
    "غصینوح",
    "غمارگح",
    "غمشهدح",
    "غمهراح",
    "غمینوح",
    "غنابح",
    "غنوشح",
    "غنیلیح",
    "غویتاح",
    "غپاکح",
    "غپینوح",
    "غچینح",
    "غگرجیح",
    "غگرگح",
    "غگلح",
    "فاذرح",
    "فاراکح",
    "فاسمینح",
    "فافزاح",
    "فالبرح",
    "فالومح",
    "فاماح",
    "فاهوازح",
    "فایراح",
    "فباهنرح",
    "فبیراح",
    "فجامح",
    "فجرح",
    "فجوشح",
    "فخاسح",
    "فخوزح",
    "فرآورح",
    "فرومح",
    "فزرینح",
    "فساح",
    "فسازانح",
    "فسدیدح",
    "فسربح",
    "فسپاح",
    "فلاتح",
    "فلامیح",
    "فلولهح",
    "فماکح",
    "فملیح",
    "فن آواح",
    "فنفتح",
    "فنوالح",
    "فنوردح",
    "فولادح",
    "فولاژح",
    "فولایح",
    "فوکاح",
    "فپنتاح",
    "قاسمح",
    "قجامح",
    "قرنح",
    "قزوینح",
    "قشرینح",
    "قشهدح",
    "قشکرح",
    "قشیرح",
    "قصفهاح",
    "قلرستح",
    "قنقشح",
    "قنیشاح",
    "قپارسح",
    "لابساح",
    "لازماح",
    "لبوتانح",
    "لخانهح",
    "لخزرح",
    "لراداح",
    "لسرماح",
    "لوتوسح",
    "لپیامح",
    "لکماح",
    "مادیراح",
    "مدارانح",
    "مرقامح",
    "معیارح",
    "ملتح",
    "میدکوح",
    "میهنح",
    "نبروجح",
    "نتوسح",
    "نشیراح",
    "نمرینوح",
    "نوینح",
    "نیروح",
    "هجرتح",
    "همراهح",
    "وآذرح",
    "وآرینح",
    "وآفریح",
    "وآیندح",
    "واتیح",
    "وارسح",
    "واعتبارح",
    "والبرح",
    "وامیدح",
    "وانصارح",
    "وبانکح",
    "وبشهرح",
    "وبملتح",
    "وبهمنح",
    "وبوعلیح",
    "وبیمهح",
    "وتجارتح",
    "وتعاونح",
    "وتوسح",
    "وتوسمح",
    "وتوسکاح",
    "وتوشهح",
    "وتوصاح",
    "وتوکاح",
    "وثوقح",
    "وحافظح",
    "وخارزمح",
    "وخاورح",
    "وداناح",
    "ودیح",
    "ورازیح",
    "ورناح",
    "وزمینح",
    "وساختح",
    "وساپاح",
    "وسدیدح",
    "وسرمدح",
    "وسناح",
    "وسپهح",
    "وسکابح",
    "وسیناح",
    "وسینح",
    "وشمالح",
    "وصناح",
    "وصندوقح",
    "وصنعتح",
    "وغدیرح",
    "وقوامح",
    "ولبهمنح",
    "ولتجارح",
    "ولرازح",
    "ولساپاح",
    "ولشرقح",
    "ولصنمح",
    "ولغدرح",
    "ولقمانح",
    "ولملتح",
    "ولیزح",
    "ومعادنح",
    "وملتح",
    "ومللح",
    "وملیح",
    "ونفتح",
    "ونوینح",
    "ونیروح",
    "ونیکیح",
    "وهامونح",
    "وهورح",
    "وپارسح",
    "وپاسارح",
    "وپتروح",
    "وپخشح",
    "وکادوح",
    "وکارح",
    "وکوثرح",
    "وگردشح",
    "پارتاح",
    "پارسانح",
    "پارسیانح",
    "پاساح",
    "پاکشوح",
    "پتایرح",
    "پترولح",
    "پخشح",
    "پدرخشح",
    "پرداختح",
    "پردیسح",
    "پسهندح",
    "پشاهنح",
    "پلاستح",
    "پلاسکح",
    "پلولهح",
    "پنکاح",
    "پکرمانح",
    "پکویرح",
    "پکیانح",
    "چافستح",
    "چفیبرح",
    "چکارلح",
    "چکارنح",
    "چکاوهح",
    "کابگنح",
    "کاذرح",
    "کازروح",
    "کاسپینح",
    "کالبرح",
    "کاماح",
    "کاوهح",
    "کایتاح",
    "کبافقح",
    "کترامح",
    "کتوکاح",
    "کحافظح",
    "کخاکح",
    "کدماح",
    "کرازیح",
    "کرماشاح",
    "کرویح",
    "کزغالح",
    "کساوهح",
    "کساپاح",
    "کسراح",
    "کسرامح",
    "کسعدیح",
    "کطبسح",
    "کفرآورح",
    "کفراح",
    "کفپارسح",
    "کقزویح",
    "کلوندح",
    "کماسهح",
    "کمرجانح",
    "کمنگنزح",
    "کمیناح",
    "کنورح",
    "کهرامح",
    "کهمداح",
    "کوثرح",
    "کورزح",
    "کویرح",
    "کپارسح",
    "کپاناح",
    "کپرورح",
    "کپشیرح",
    "کچادح",
    "کگازح",
    "کگلح",
    "کگهرح",
    "کی بی سیح",
    "کیسونح",
    "گوهرانح",
    "گکیشح",
]
right_offers = [(i,) for i in right_offers]
right_offers = spark.createDataFrame(data=right_offers, schema=valid_symbols_df.schema)
valid_symbols_df = valid_symbols_df.union(right_offers).dropDuplicates()
display_df(valid_symbols_df)
#%%

# Adding some other symbols valid symbols
handly_collected_valid_symbols = ["فسلير", "نگین", "نیرو", "غگز", "آینده"]
handly_collected_valid_symbols = [(i,) for i in handly_collected_valid_symbols]
handly_collected_valid_symbols = spark.createDataFrame(
    data = handly_collected_valid_symbols, schema = valid_symbols_df.schema
)
valid_symbols_df = valid_symbols_df.union(
    handly_collected_valid_symbols
).dropDuplicates()
display_df(valid_symbols_df)
#%%

# removing invalid symbols
invalid_symbols = [
    "وکوثر",
    "حکمت",
    "ومهر",
    "جوین",
    "وقوام",
    "ممسنی",
    "غناب",
    "کارا",
    "کاوه",
    "گنگین",
    "وپویا",
    "ولپارس",
    "نوری",
    "شجی",
    "ثعمسا",
    "بگیلان",
    "بجهرم",
    "آرمانح",
    "شساخت",
    "ثخوز",
    # 'قاروم', # capital increase
    "امید",
    "وهنر",
    "تنوین",
    "وگردش",
    "آریان",  # two different stocks with the same symbol
    "وآتوس",
    "ودانا",
    "وآیند",
    "آکنتور",
    "کمینا",  # invalid in this period
    "ومشان",  # invalid in this period
    # 'همراه', # capital increase!
]

valid_symbols_df = valid_symbols_df.filter(~F.col("symbol").isin(invalid_symbols))
display_df(valid_symbols_df)
#%%

# saving the dataframe into parquet files
valid_symbols_df.write.mode("overwrite").parquet(
    VALID_SYMBOLS_PATH + "/{}".format("validSymbols.parquet")
)

# %%
