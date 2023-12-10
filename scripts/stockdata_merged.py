import pandas as pd
from datetime import datetime, timedelta

# Start date
start_date = datetime.strptime("25-11-2019", "%d-%m-%Y")

# End date
end_date = datetime.strptime("28-12-2022", "%d-%m-%Y")

# Generate weekly date ranges
weeks = pd.date_range(start=start_date, end=end_date, freq='W-Mon')

files = ['stockmarket_data/A.csv', 'stockmarket_data/AAALY.csv', 'stockmarket_data/AACAY.csv',
         'stockmarket_data/AAL.csv', 'stockmarket_data/AAP.csv', 'stockmarket_data/AAPL.csv',
         'stockmarket_data/ABBV.csv', 'stockmarket_data/ABC.csv', 'stockmarket_data/ABLT.csv',
         'stockmarket_data/ABT.csv', 'stockmarket_data/ABTZY.csv', 'stockmarket_data/ACCYY.csv',
         'stockmarket_data/ACDVF.csv', 'stockmarket_data/ACFL.csv', 'stockmarket_data/ACGBY.csv',
         'stockmarket_data/ACGL.csv', 'stockmarket_data/ACN.csv', 'stockmarket_data/ACXIF.csv',
         'stockmarket_data/ADDYY.csv', 'stockmarket_data/ADERY.csv', 'stockmarket_data/ADI.csv',
         'stockmarket_data/ADM.csv', 'stockmarket_data/ADP.csv', 'stockmarket_data/ADSK.csv',
         'stockmarket_data/AEHR.csv', 'stockmarket_data/AEL.csv', 'stockmarket_data/AER.csv',
         'stockmarket_data/AET.csv', 'stockmarket_data/AFG.csv', 'stockmarket_data/AFLYY.csv',
         'stockmarket_data/AFSI.csv', 'stockmarket_data/AGNC.csv', 'stockmarket_data/AGO.csv',
         'stockmarket_data/AGPYY.csv', 'stockmarket_data/AGRUF.csv', 'stockmarket_data/AGZNF.csv',
         'stockmarket_data/AHCHY.csv', 'stockmarket_data/AHKSY.csv', 'stockmarket_data/AHODF.csv',
         'stockmarket_data/AIAGF.csv', 'stockmarket_data/AIN.csv', 'stockmarket_data/AIPUY.csv',
         'stockmarket_data/AIRI.csv', 'stockmarket_data/AIZ.csv', 'stockmarket_data/AJG.csv',
         'stockmarket_data/AJINY.csv', 'stockmarket_data/AKAM.csv', 'stockmarket_data/AKBTY.csv',
         'stockmarket_data/AKZOY.csv', 'stockmarket_data/ALB.csv', 'stockmarket_data/ALBKY.csv',
         'stockmarket_data/ALIOF.csv', 'stockmarket_data/ALK.csv', 'stockmarket_data/ALLY.csv',
         'stockmarket_data/ALPMY.csv', 'stockmarket_data/ALR.csv', 'stockmarket_data/ALSMY.csv',
         'stockmarket_data/ALV.csv', 'stockmarket_data/AMADY.csv', 'stockmarket_data/AMAT.csv',
         'stockmarket_data/AMBZ.csv', 'stockmarket_data/AMD.csv', 'stockmarket_data/AMG.csv',
         'stockmarket_data/AMGN.csv', 'stockmarket_data/AMKBY.csv', 'stockmarket_data/AMP.csv',
         'stockmarket_data/AMT.csv', 'stockmarket_data/AMTD.csv', 'stockmarket_data/AMX.csv',
         'stockmarket_data/AMZN.csv', 'stockmarket_data/AN.csv', 'stockmarket_data/ANDE.csv',
         'stockmarket_data/AON.csv', 'stockmarket_data/APAJF.csv', 'stockmarket_data/APD.csv',
         'stockmarket_data/APEMY.csv', 'stockmarket_data/ARCC.csv', 'stockmarket_data/AREN.csv',
         'stockmarket_data/ARKAY.csv', 'stockmarket_data/ARMK.csv', 'stockmarket_data/ARW.csv',
         'stockmarket_data/ASB.csv', 'stockmarket_data/ASBFF.csv', 'stockmarket_data/ASBRF.csv',
         'stockmarket_data/ASEKY.csv', 'stockmarket_data/ASGLY.csv', 'stockmarket_data/ASHTF.csv',
         'stockmarket_data/ASML.csv', 'stockmarket_data/ASX.csv', 'stockmarket_data/ATASY.csv',
         'stockmarket_data/ATNNF.csv', 'stockmarket_data/ATONF.csv', 'stockmarket_data/ATVI.csv',
         'stockmarket_data/AUNB.csv', 'stockmarket_data/AUTR.csv', 'stockmarket_data/AVB.csv',
         'stockmarket_data/AVGO.csv', 'stockmarket_data/AVT.csv', 'stockmarket_data/AVY.csv',
         'stockmarket_data/AWK.csv', 'stockmarket_data/AXP.csv', 'stockmarket_data/AXXTF.csv',
         'stockmarket_data/AZN.csv', 'stockmarket_data/AZO.csv', 'stockmarket_data/BAESY.csv',
         'stockmarket_data/BAK.csv', 'stockmarket_data/BAM.csv', 'stockmarket_data/BANGN.csv',
         'stockmarket_data/BAP.csv', 'stockmarket_data/BARK.csv', 'stockmarket_data/BASFY.csv', 'stockmarket_data/BAX.csv', 'stockmarket_data/BBBY.csv', 'stockmarket_data/BBD.csv', 'stockmarket_data/BBRYF.csv',
    'stockmarket_data/BBVA.csv', 'stockmarket_data/BBY.csv', 'stockmarket_data/BCBHF.csv', 'stockmarket_data/BCH.csv',
    'stockmarket_data/BCHHF.csv', 'stockmarket_data/BCKIY.csv', 'stockmarket_data/BCMXY.csv', 'stockmarket_data/BCS.csv',
    'stockmarket_data/BDORY.csv', 'stockmarket_data/BDOUY.csv', 'stockmarket_data/BDRBF.csv', 'stockmarket_data/BDX.csv',
    'stockmarket_data/BEN.csv', 'stockmarket_data/BF-A.csv', 'stockmarket_data/BGAOY.csv', 'stockmarket_data/BHI.csv',
    'stockmarket_data/BIDU.csv', 'stockmarket_data/BIIB.csv', 'stockmarket_data/BIRDF.csv', 'stockmarket_data/BK.csv',
    'stockmarket_data/BKHYY.csv', 'stockmarket_data/BKNIY.csv', 'stockmarket_data/BKQNY.csv', 'stockmarket_data/BKRKY.csv',
    'stockmarket_data/BKRL.csv', 'stockmarket_data/BKU.csv', 'stockmarket_data/BKUT.csv', 'stockmarket_data/BLD.csv',
    'stockmarket_data/BLK.csv', 'stockmarket_data/BLSFF.csv', 'stockmarket_data/BMBLF.csv', 'stockmarket_data/BMO.csv',
    'stockmarket_data/BMRA.csv', 'stockmarket_data/BMRN.csv', 'stockmarket_data/BMY.csv', 'stockmarket_data/BNDSY.csv',
    'stockmarket_data/BNGRF.csv', 'stockmarket_data/BNPQY.csv', 'stockmarket_data/BNS.csv', 'stockmarket_data/BNSO.csv',
    'stockmarket_data/BNTGY.csv', 'stockmarket_data/BORT.csv', 'stockmarket_data/BOTRF.csv', 'stockmarket_data/BOUYY.csv',
    'stockmarket_data/BPCGF.csv', 'stockmarket_data/BPIRY.csv', 'stockmarket_data/BPOP.csv', 'stockmarket_data/BPXXY.csv',
    'stockmarket_data/BRDCY.csv', 'stockmarket_data/BRK-A.csv', 'stockmarket_data/BRTHY.csv', 'stockmarket_data/BSAC.csv',
    'stockmarket_data/BSX.csv', 'stockmarket_data/BTDPF.csv', 'stockmarket_data/BTI.csv', 'stockmarket_data/BUD.csv',
    'stockmarket_data/BWA.csv', 'stockmarket_data/BXP.csv', 'stockmarket_data/BXRBF.csv', 'stockmarket_data/BYCBF.csv',
    'stockmarket_data/BZLFY.csv', 'stockmarket_data/C.csv', 'stockmarket_data/CAA.csv', 'stockmarket_data/CAAS.csv',
    'stockmarket_data/CABGY.csv', 'stockmarket_data/CAG.csv', 'stockmarket_data/CAH.csv', 'stockmarket_data/CAIXY.csv',
    'stockmarket_data/CAR.csv', 'stockmarket_data/CASH.csv', 'stockmarket_data/CAT.csv', 'stockmarket_data/CBAOF.csv',
    'stockmarket_data/CBD.csv', 'stockmarket_data/CBI.csv', 'stockmarket_data/CBLUY.csv', 'stockmarket_data/CBOF.csv',
    'stockmarket_data/CBSH.csv', 'stockmarket_data/CBUMF.csv', 'stockmarket_data/CBX.csv', 'stockmarket_data/CCBC.csv',
    'stockmarket_data/CCE.csv', 'stockmarket_data/CCGDF.csv', 'stockmarket_data/CCGY.csv', 'stockmarket_data/CCHGY.csv',
    'stockmarket_data/CCI.csv', 'stockmarket_data/CCK.csv', 'stockmarket_data/CCOHF.csv', 'stockmarket_data/CDEVY.csv',
    'stockmarket_data/CDUAF.csv', 'stockmarket_data/CEA.csv', 'stockmarket_data/CESTF.csv', 'stockmarket_data/CET.csv',
    'stockmarket_data/CEVIY.csv', 'stockmarket_data/CFEGF.csv', 'stockmarket_data/CFG.csv', 'stockmarket_data/CFR.csv',
    'stockmarket_data/CGEMY.csv', 'stockmarket_data/CGKEY.csv', 'stockmarket_data/CGMBF.csv', 'stockmarket_data/CGYV.csv',
    'stockmarket_data/CHCC.csv', 'stockmarket_data/CHD.csv', 'stockmarket_data/CHDRY.csv', 'stockmarket_data/CHHE.csv', 'stockmarket_data/CHKGF.csv', 'stockmarket_data/CHKP.csv', 'stockmarket_data/CHNGQ.csv',
    'stockmarket_data/CHPXF.csv', 'stockmarket_data/CHRW.csv', 'stockmarket_data/CHT.csv', 'stockmarket_data/CHTR.csv',
    'stockmarket_data/CHUEF.csv', 'stockmarket_data/CHWE.csv', 'stockmarket_data/CIADY.csv', 'stockmarket_data/CIB.csv',
    'stockmarket_data/CICHY.csv', 'stockmarket_data/CIHKY.csv', 'stockmarket_data/CIM.csv', 'stockmarket_data/CIMDF.csv',
    'stockmarket_data/CINF.csv', 'stockmarket_data/CINSF.csv', 'stockmarket_data/CIOXY.csv', 'stockmarket_data/CJEWF.csv',
    'stockmarket_data/CJPRF.csv', 'stockmarket_data/CL.csv', 'stockmarket_data/CLPBY.csv', 'stockmarket_data/CLPXY.csv',
    'stockmarket_data/CLR.csv', 'stockmarket_data/CM.csv', 'stockmarket_data/CMCLF.csv', 'stockmarket_data/CME.csv',
    'stockmarket_data/CMGGF.csv', 'stockmarket_data/CMHHF.csv', 'stockmarket_data/CMI.csv', 'stockmarket_data/CMPGY.csv',
    'stockmarket_data/CMPNF.csv', 'stockmarket_data/CMTDF.csv', 'stockmarket_data/CMWAY.csv', 'stockmarket_data/CNHI.csv',
    'stockmarket_data/CNI.csv', 'stockmarket_data/CNO.csv', 'stockmarket_data/CNP.csv', 'stockmarket_data/CNQ.csv',
    'stockmarket_data/CNRAF.csv', 'stockmarket_data/COL.csv', 'stockmarket_data/COP.csv', 'stockmarket_data/COST.csv',
    'stockmarket_data/COTY.csv', 'stockmarket_data/CP.csv', 'stockmarket_data/CPB.csv', 'stockmarket_data/CPCAY.csv',
    'stockmarket_data/CPICQ.csv', 'stockmarket_data/CPWHF.csv', 'stockmarket_data/CPYYF.csv', 'stockmarket_data/CRARY.csv',
    'stockmarket_data/CRBJF.csv', 'stockmarket_data/CRCBY.csv', 'stockmarket_data/CRM.csv', 'stockmarket_data/CRRFY.csv',
    'stockmarket_data/CRZBY.csv', 'stockmarket_data/CS.csv', 'stockmarket_data/CSASF.csv', 'stockmarket_data/CSCO.csv',
    'stockmarket_data/CSIQ.csv', 'stockmarket_data/CSSI.csv', 'stockmarket_data/CSUAY.csv', 'stockmarket_data/CSWYY.csv', 'stockmarket_data/CTBK.csv', 'stockmarket_data/CTC.csv',
    'stockmarket_data/CTRYF.csv', 'stockmarket_data/CTXAY.csv', 'stockmarket_data/CUK.csv', 'stockmarket_data/CVE.csv',
    'stockmarket_data/CYH.csv', 'stockmarket_data/DAHSF.csv', 'stockmarket_data/DAL.csv', 'stockmarket_data/DANOY.csv',
    'stockmarket_data/DASTY.csv', 'stockmarket_data/DCNSF.csv', 'stockmarket_data/DE.csv', 'stockmarket_data/DEO.csv',
    'stockmarket_data/DFS.csv', 'stockmarket_data/DG.csv', 'stockmarket_data/DGX.csv', 'stockmarket_data/DHI.csv',
    'stockmarket_data/DIS.csv', 'stockmarket_data/DISH.csv', 'stockmarket_data/DITTF.csv', 'stockmarket_data/DKILF.csv',
    'stockmarket_data/DKSHF.csv', 'stockmarket_data/DLAKY.csv', 'stockmarket_data/DLMAF.csv', 'stockmarket_data/DLTR.csv',
    'stockmarket_data/DNFGY.csv', 'stockmarket_data/DNPCF.csv', 'stockmarket_data/DNTUF.csv', 'stockmarket_data/DOW.csv',
    'stockmarket_data/DOX.csv', 'stockmarket_data/DPSGY.csv', 'stockmarket_data/DRI.csv', 'stockmarket_data/DSDVY.csv',
    'stockmarket_data/DSEEY.csv', 'stockmarket_data/DSITF.csv', 'stockmarket_data/DSNKY.csv', 'stockmarket_data/DTE.csv',
    'stockmarket_data/DTEGY.csv', 'stockmarket_data/DUAVF.csv', 'stockmarket_data/DUFRY.csv', 'stockmarket_data/DVA.csv',
    'stockmarket_data/DWAHF.csv', 'stockmarket_data/DWHHF.csv', 'stockmarket_data/EA.csv', 'stockmarket_data/EBAY.csv',
    'stockmarket_data/EBKDY.csv', 'stockmarket_data/EC.csv', 'stockmarket_data/ECL.csv', 'stockmarket_data/ED.csv',
    'stockmarket_data/EDPFY.csv', 'stockmarket_data/EDRWY.csv', 'stockmarket_data/EFGSY.csv', 'stockmarket_data/EFGXY.csv',
    'stockmarket_data/EFX.csv', 'stockmarket_data/EGFEY.csv', 'stockmarket_data/EGRNY.csv', 'stockmarket_data/EIX.csv',
    'stockmarket_data/EJPRY.csv', 'stockmarket_data/ELPQF.csv', 'stockmarket_data/ELROF.csv', 'stockmarket_data/ELUXY.csv',
    'stockmarket_data/ELVAF.csv', 'stockmarket_data/EMN.csv', 'stockmarket_data/EMR.csv', 'stockmarket_data/EMSHF.csv',
    'stockmarket_data/ENB.csv', 'stockmarket_data/ENGIY.csv', 'stockmarket_data/ENLAY.csv', 'stockmarket_data/ENS.csv',
    'stockmarket_data/EOG.csv', 'stockmarket_data/EONGY.csv', 'stockmarket_data/EPR.csv', 'stockmarket_data/EQIX.csv',
    'stockmarket_data/EQR.csv', 'stockmarket_data/ES.csv', 'stockmarket_data/ESRX.csv', 'stockmarket_data/ESS.csv',
    'stockmarket_data/ESYJY.csv', 'stockmarket_data/EUZOF.csv', 'stockmarket_data/EW.csv', 'stockmarket_data/EWBC.csv',
    'stockmarket_data/EXPE.csv', 'stockmarket_data/EXPGY.csv', 'stockmarket_data/EXR.csv', 'stockmarket_data/F.csv', 'stockmarket_data/FAST.csv', 'stockmarket_data/FBHS.csv',
    'stockmarket_data/FCNCA.csv', 'stockmarket_data/FCODF.csv', 'stockmarket_data/FCX.csv', 'stockmarket_data/FE.csv',
    'stockmarket_data/FFIV.csv', 'stockmarket_data/FHN.csv', 'stockmarket_data/FINMY.csv', 'stockmarket_data/FIS.csv',
    'stockmarket_data/FISV.csv', 'stockmarket_data/FITB.csv', 'stockmarket_data/FIZZ.csv', 'stockmarket_data/FJTSY.csv',
    'stockmarket_data/FKKFY.csv', 'stockmarket_data/FL.csv', 'stockmarket_data/FLT.csv', 'stockmarket_data/FNF.csv',
    'stockmarket_data/FNLPF.csv', 'stockmarket_data/FOJCY.csv', 'stockmarket_data/FOSUF.csv', 'stockmarket_data/FOXA.csv',
    'stockmarket_data/FPAFY.csv', 'stockmarket_data/FQVLF.csv', 'stockmarket_data/FRBA.csv', 'stockmarket_data/FRC.csv',
    'stockmarket_data/FRCOY.csv', 'stockmarket_data/FRRVF.csv', 'stockmarket_data/FRZCF.csv', 'stockmarket_data/FSUMF.csv',
    'stockmarket_data/GAILF.csv', 'stockmarket_data/GARPF.csv', 'stockmarket_data/GBERY.csv', 'stockmarket_data/GBNK.csv',
    'stockmarket_data/GCHOY.csv', 'stockmarket_data/GCLMF.csv', 'stockmarket_data/GD.csv', 'stockmarket_data/GE.csv',
    'stockmarket_data/GECFF.csv', 'stockmarket_data/GELYY.csv', 'stockmarket_data/GGAL.csv', 'stockmarket_data/GGDVF.csv',
    'stockmarket_data/GGNPF.csv', 'stockmarket_data/GIB.csv', 'stockmarket_data/GILD.csv', 'stockmarket_data/GIS.csv',
    'stockmarket_data/GJNSF.csv', 'stockmarket_data/GKNLY.csv', 'stockmarket_data/GLHD.csv', 'stockmarket_data/GLPEF.csv',
    'stockmarket_data/GM.csv', 'stockmarket_data/GME.csv', 'stockmarket_data/GMELF.csv', 'stockmarket_data/GMGSF.csv',
    'stockmarket_data/GNGBY.csv', 'stockmarket_data/GNRC.csv', 'stockmarket_data/GNW.csv', 'stockmarket_data/GNZUF.csv',
    'stockmarket_data/GOOG.csv', 'stockmarket_data/GPC.csv', 'stockmarket_data/GPI.csv', 'stockmarket_data/GPN.csv',
    'stockmarket_data/GPTGF.csv', 'stockmarket_data/GRFS.csv', 'stockmarket_data/GRMN.csv', 'stockmarket_data/GRSXY.csv',
    'stockmarket_data/GS-PJ.csv', 'stockmarket_data/GSEFF.csv', 'stockmarket_data/GSK.csv', 'stockmarket_data/GTWCF.csv',
    'stockmarket_data/GVDNY.csv', 'stockmarket_data/GWLLY.csv', 'stockmarket_data/GWW.csv', 'stockmarket_data/HACBY.csv',
    'stockmarket_data/HAL.csv', 'stockmarket_data/HALFF.csv', 'stockmarket_data/HAS.csv', 'stockmarket_data/HBAN.csv',
    'stockmarket_data/HBI.csv', 'stockmarket_data/HCMLY.csv', 'stockmarket_data/HD.csv', 'stockmarket_data/HDB.csv',
    'stockmarket_data/HDELY.csv', 'stockmarket_data/HEOFF.csv', 'stockmarket_data/HERB.csv', 'stockmarket_data/HFBK.csv',
    'stockmarket_data/HGKGF.csv', 'stockmarket_data/HHRBF.csv', 'stockmarket_data/HII.csv', 'stockmarket_data/HKHHY.csv',
    'stockmarket_data/HKUOF.csv', 'stockmarket_data/HKXCF.csv', 'stockmarket_data/HLFDF.csv', 'stockmarket_data/HLRTF.csv',
    'stockmarket_data/HMC.csv', 'stockmarket_data/HMNF.csv', 'stockmarket_data/HNHPF.csv', 'stockmarket_data/HNLGF.csv',
    'stockmarket_data/HNTIF.csv', 'stockmarket_data/HOG.csv', 'stockmarket_data/HOLX.csv', 'stockmarket_data/HON.csv',
    'stockmarket_data/HPE.csv', 'stockmarket_data/HPIL.csv', 'stockmarket_data/HRL.csv', 'stockmarket_data/HRNNF.csv',
    'stockmarket_data/HSBC.csv', 'stockmarket_data/HSIC.csv', 'stockmarket_data/HST.csv', 'stockmarket_data/HTHIY.csv',
    'stockmarket_data/HTNGF.csv', 'stockmarket_data/HTZ.csv', 'stockmarket_data/HUM.csv', 'stockmarket_data/HXGBY.csv',
    'stockmarket_data/HYKUF.csv', 'stockmarket_data/HYMTF.csv', 'stockmarket_data/HYUHF.csv', 'stockmarket_data/IAUGY.csv',
    'stockmarket_data/IBDRY.csv', 'stockmarket_data/IBKR.csv', 'stockmarket_data/IBN.csv', 'stockmarket_data/ICE.csv',
    'stockmarket_data/ICL.csv', 'stockmarket_data/IDKOF.csv', 'stockmarket_data/IDLLF.csv', 'stockmarket_data/IESFY.csv',
    'stockmarket_data/IFCZF.csv', 'stockmarket_data/IFF.csv', 'stockmarket_data/IFNNY.csv', 'stockmarket_data/IFSB.csv',
    'stockmarket_data/IGGHY.csv', 'stockmarket_data/IGPPF.csv', 'stockmarket_data/IHCPF.csv', 'stockmarket_data/IHG.csv', 'stockmarket_data/IITSF.csv', 'stockmarket_data/ILMN.csv', 'stockmarket_data/IMHDF.csv',
    'stockmarket_data/INFY.csv', 'stockmarket_data/INT.csv', 'stockmarket_data/INTK.csv', 'stockmarket_data/INTL.csv',
    'stockmarket_data/INTU.csv', 'stockmarket_data/IP.csv', 'stockmarket_data/IPOAF.csv', 'stockmarket_data/IR.csv',
    'stockmarket_data/ISRG.csv', 'stockmarket_data/ISUZY.csv', 'stockmarket_data/ITUB.csv', 'stockmarket_data/ITW.csv',
    'stockmarket_data/IVSXF.csv', 'stockmarket_data/IVTJF.csv', 'stockmarket_data/IVZ.csv', 'stockmarket_data/JAPAF.csv',
    'stockmarket_data/JAPSY.csv', 'stockmarket_data/JBAXY.csv', 'stockmarket_data/JBHT.csv', 'stockmarket_data/JBL.csv',
    'stockmarket_data/JBLU.csv', 'stockmarket_data/JCI.csv', 'stockmarket_data/JD.csv', 'stockmarket_data/JDID.csv',
    'stockmarket_data/JEXYF.csv', 'stockmarket_data/JFEEF.csv', 'stockmarket_data/JGSMY.csv', 'stockmarket_data/JMHLY.csv',
    'stockmarket_data/JMPLY.csv', 'stockmarket_data/JNJ.csv', 'stockmarket_data/JNPR.csv', 'stockmarket_data/JPHLF.csv',
    'stockmarket_data/JPM.csv', 'stockmarket_data/JRONY.csv', 'stockmarket_data/JSNSF.csv', 'stockmarket_data/JWN.csv',
    'stockmarket_data/JXHLY.csv', 'stockmarket_data/JYSKY.csv', 'stockmarket_data/K.csv', 'stockmarket_data/KBCSY.csv',
    'stockmarket_data/KBDCY.csv', 'stockmarket_data/KBSTF.csv', 'stockmarket_data/KE.csv', 'stockmarket_data/KEP.csv',
    'stockmarket_data/KEY.csv', 'stockmarket_data/KFRC.csv', 'stockmarket_data/KGFHY.csv', 'stockmarket_data/KHC.csv',
    'stockmarket_data/KHNGY.csv', 'stockmarket_data/KKOYF.csv', 'stockmarket_data/KLAC.csv', 'stockmarket_data/KLMR.csv',
    'stockmarket_data/KLPEF.csv', 'stockmarket_data/KMAAF.csv', 'stockmarket_data/KMB.csv', 'stockmarket_data/KMTUY.csv',
    'stockmarket_data/KMX.csv', 'stockmarket_data/KOTMY.csv', 'stockmarket_data/KR.csv', 'stockmarket_data/KREVF.csv',
    'stockmarket_data/KRNNF.csv', 'stockmarket_data/KRYAF.csv', 'stockmarket_data/KRYPF.csv', 'stockmarket_data/KWGPF.csv',
    'stockmarket_data/KWHIY.csv', 'stockmarket_data/KYSEF.csv', 'stockmarket_data/LBRDA.csv', 'stockmarket_data/LBTYA.csv',
    'stockmarket_data/LDNXF.csv', 'stockmarket_data/LGFRY.csv', 'stockmarket_data/LGGNY.csv', 'stockmarket_data/LGRDY.csv',
    'stockmarket_data/LILA.csv', 'stockmarket_data/LLESF.csv', 'stockmarket_data/LLY.csv', 'stockmarket_data/LMT.csv',
    'stockmarket_data/LNC.csv', 'stockmarket_data/LNDZF.csv', 'stockmarket_data/LNG.csv', 'stockmarket_data/LNT.csv',
    'stockmarket_data/LNVGY.csv', 'stockmarket_data/LPL.csv', 'stockmarket_data/LRCDF.csv', 'stockmarket_data/LRCX.csv',
    'stockmarket_data/LSGOF.csv', 'stockmarket_data/LTOUF.csv', 'stockmarket_data/LUV.csv', 'stockmarket_data/LVS.csv',
    'stockmarket_data/LYB.csv', 'stockmarket_data/LYG.csv', 'stockmarket_data/LYV.csv', 'stockmarket_data/LZAGF.csv',
    'stockmarket_data/M.csv', 'stockmarket_data/MA.csv', 'stockmarket_data/MAA.csv', 'stockmarket_data/MAANF.csv', 'stockmarket_data/MAEOY.csv',
    'stockmarket_data/MAHMF.csv', 'stockmarket_data/MAKSY.csv', 'stockmarket_data/MAN.csv', 'stockmarket_data/MAR.csv',
    'stockmarket_data/MAT.csv', 'stockmarket_data/MCD.csv', 'stockmarket_data/MCHP.csv', 'stockmarket_data/MCHVY.csv',
    'stockmarket_data/MDEVF.csv', 'stockmarket_data/MDLZ.csv', 'stockmarket_data/MDT.csv', 'stockmarket_data/MEJHF.csv',
    'stockmarket_data/MET.csv', 'stockmarket_data/MFG.csv', 'stockmarket_data/MGA.csv', 'stockmarket_data/MGM.csv',
    'stockmarket_data/MHK.csv', 'stockmarket_data/MHVYF.csv', 'stockmarket_data/MIELY.csv', 'stockmarket_data/MIMTF.csv',
    'stockmarket_data/MITEY.csv', 'stockmarket_data/MITUF.csv', 'stockmarket_data/MIUFY.csv', 'stockmarket_data/MLLUY.csv',
    'stockmarket_data/MLM.csv', 'stockmarket_data/MMC.csv', 'stockmarket_data/MMMKF.csv', 'stockmarket_data/MMTOF.csv',
    'stockmarket_data/MNST.csv', 'stockmarket_data/MO.csv', 'stockmarket_data/MOH.csv', 'stockmarket_data/MON.csv',
    'stockmarket_data/MONDY.csv', 'stockmarket_data/MPC.csv', 'stockmarket_data/MPFRF.csv', 'stockmarket_data/MQBKY.csv',
    'stockmarket_data/MRAAY.csv', 'stockmarket_data/MRO.csv', 'stockmarket_data/MRPRF.csv', 'stockmarket_data/MRVGF.csv',
    'stockmarket_data/MS-PF.csv', 'stockmarket_data/MSFT.csv', 'stockmarket_data/MSI.csv', 'stockmarket_data/MSLOF.csv',
    'stockmarket_data/MT.csv', 'stockmarket_data/MTD.csv', 'stockmarket_data/MTPOF.csv', 'stockmarket_data/MTRAF.csv',
    'stockmarket_data/MTSFF.csv', 'stockmarket_data/MU.csv', 'stockmarket_data/MUSA.csv', 'stockmarket_data/MZDAY.csv',
    'stockmarket_data/NABZY.csv', 'stockmarket_data/NBGGY.csv', 'stockmarket_data/NCCGF.csv', 'stockmarket_data/NCLH.csv',
    'stockmarket_data/NCLTF.csv', 'stockmarket_data/NCMGY.csv', 'stockmarket_data/NDEKY.csv', 'stockmarket_data/NEE.csv',
    'stockmarket_data/NEM.csv', 'stockmarket_data/NEWP.csv', 'stockmarket_data/NFLX.csv', 'stockmarket_data/NGG.csv',
    'stockmarket_data/NGLOY.csv', 'stockmarket_data/NHYDY.csv', 'stockmarket_data/NI.csv', 'stockmarket_data/NIPMY.csv',
    'stockmarket_data/NISTF.csv', 'stockmarket_data/NLY.csv', 'stockmarket_data/NMHLY.csv', 'stockmarket_data/NNGPF.csv',
    'stockmarket_data/NOBGF.csv', 'stockmarket_data/NOC.csv', 'stockmarket_data/NOV.csv', 'stockmarket_data/NOW.csv',
    'stockmarket_data/NPSNY.csv', 'stockmarket_data/NRG.csv', 'stockmarket_data/NRILY.csv', 'stockmarket_data/NRTHF.csv',
    'stockmarket_data/NRVTF.csv', 'stockmarket_data/NSANY.csv', 'stockmarket_data/NSC.csv', 'stockmarket_data/NTAP.csv',
    'stockmarket_data/NTCXF.csv', 'stockmarket_data/NTDOY.csv', 'stockmarket_data/NTES.csv', 'stockmarket_data/NTIOF.csv',
    'stockmarket_data/NTOIY.csv', 'stockmarket_data/NTRS.csv', 'stockmarket_data/NVO.csv', 'stockmarket_data/NVS.csv',
    'stockmarket_data/NVZMY.csv', 'stockmarket_data/NWL.csv', 'stockmarket_data/NWYF.csv', 'stockmarket_data/NXGPY.csv',
    'stockmarket_data/NXPI.csv', 'stockmarket_data/NYCB.csv', 'stockmarket_data/O.csv', 'stockmarket_data/OAOFY.csv',
    'stockmarket_data/OC.csv', 'stockmarket_data/ODMUF.csv', 'stockmarket_data/ODP.csv', 'stockmarket_data/OGFGF.csv',
    'stockmarket_data/OJOC.csv', 'stockmarket_data/OLCLY.csv', 'stockmarket_data/OMC.csv', 'stockmarket_data/ORAN.csv',
    'stockmarket_data/ORENF.csv', 'stockmarket_data/ORI.csv', 'stockmarket_data/ORKLY.csv', 'stockmarket_data/ORLY.csv',
    'stockmarket_data/ORMP.csv', 'stockmarket_data/ORYX.csv', 'stockmarket_data/OSCUF.csv', 'stockmarket_data/OSGSF.csv',
    'stockmarket_data/OTEX.csv', 'stockmarket_data/OTPGF.csv', 'stockmarket_data/OTSKY.csv', 'stockmarket_data/OVCHF.csv',
    'stockmarket_data/OXY.csv', 'stockmarket_data/PACW.csv', 'stockmarket_data/PAG.csv', 'stockmarket_data/PAYX.csv',
    'stockmarket_data/PBA.csv', 'stockmarket_data/PBCO.csv', 'stockmarket_data/PBCRY.csv', 'stockmarket_data/PBF.csv',
    'stockmarket_data/PBLOF.csv', 'stockmarket_data/PBNK.csv', 'stockmarket_data/PBSFF.csv', 'stockmarket_data/PEG.csv',
    'stockmarket_data/PEP.csv', 'stockmarket_data/PEXUF.csv', 'stockmarket_data/PFE.csv', 'stockmarket_data/PFGC.csv',
    'stockmarket_data/PG.csv', 'stockmarket_data/PGCPF.csv', 'stockmarket_data/PGPEF.csv', 'stockmarket_data/PGPHF.csv',
    'stockmarket_data/PH.csv', 'stockmarket_data/PHM.csv', 'stockmarket_data/PHOJY.csv', 'stockmarket_data/PINC.csv', 'stockmarket_data/PKG.csv', 'stockmarket_data/PKIN.csv', 'stockmarket_data/PLD.csv',
    'stockmarket_data/PM.csv', 'stockmarket_data/PMHV.csv', 'stockmarket_data/PNR.csv', 'stockmarket_data/PNWRF.csv',
    'stockmarket_data/POAHY.csv', 'stockmarket_data/PPRUY.csv', 'stockmarket_data/PRDSF.csv', 'stockmarket_data/PRMRF.csv',
    'stockmarket_data/PRU.csv', 'stockmarket_data/PSMMY.csv', 'stockmarket_data/PSO.csv', 'stockmarket_data/PSX.csv',
    'stockmarket_data/PTBRY.csv', 'stockmarket_data/PTTTS.csv', 'stockmarket_data/PUK.csv', 'stockmarket_data/PULS.csv',
    'stockmarket_data/PVNC.csv', 'stockmarket_data/PWCDF.csv', 'stockmarket_data/PX.csv', 'stockmarket_data/PXD.csv',
    'stockmarket_data/QBEIF.csv', 'stockmarket_data/QSR.csv', 'stockmarket_data/QUBSF.csv', 'stockmarket_data/QUCCF.csv',
    'stockmarket_data/QUOT.csv', 'stockmarket_data/RACE.csv', 'stockmarket_data/RADA.csv', 'stockmarket_data/RAIFF.csv',
    'stockmarket_data/RANJY.csv', 'stockmarket_data/RANKF.csv', 'stockmarket_data/RBGLY.csv', 'stockmarket_data/RCI.csv',
    'stockmarket_data/RCL.csv', 'stockmarket_data/RCRRF.csv', 'stockmarket_data/RE.csv', 'stockmarket_data/REGN.csv',
    'stockmarket_data/REPYY.csv', 'stockmarket_data/REVB.csv', 'stockmarket_data/RF.csv', 'stockmarket_data/RFIL.csv',
    'stockmarket_data/RGA.csv', 'stockmarket_data/RH.csv', 'stockmarket_data/RHHBY.csv', 'stockmarket_data/RIG.csv',
    'stockmarket_data/RIO.csv', 'stockmarket_data/RIOCF.csv', 'stockmarket_data/RJF.csv', 'stockmarket_data/RKUNF.csv',
    'stockmarket_data/RMGOF.csv', 'stockmarket_data/RMYHY.csv', 'stockmarket_data/RNA.csv', 'stockmarket_data/RNECY.csv',
    'stockmarket_data/RNLSY.csv', 'stockmarket_data/RNR.csv', 'stockmarket_data/ROK.csv', 'stockmarket_data/ROST.csv',
    'stockmarket_data/ROYMY.csv', 'stockmarket_data/RS.csv', 'stockmarket_data/RSG.csv', 'stockmarket_data/RSNHF.csv',
    'stockmarket_data/RXEEY.csv', 'stockmarket_data/RXMD.csv', 'stockmarket_data/RY.csv', 'stockmarket_data/RYAAY.csv',
    'stockmarket_data/RYCEY.csv', 'stockmarket_data/SAFRY.csv', 'stockmarket_data/SAHN.csv', 'stockmarket_data/SAP.csv',
    'stockmarket_data/SAPIF.csv', 'stockmarket_data/SAUHF.csv', 'stockmarket_data/SAXPF.csv', 'stockmarket_data/SBGSF.csv',
    'stockmarket_data/SBHGF.csv', 'stockmarket_data/SBKFF.csv', 'stockmarket_data/SBKO.csv', 'stockmarket_data/SBNY.csv',
    'stockmarket_data/SBUX.csv', 'stockmarket_data/SCBFF.csv', 'stockmarket_data/SCFLF.csv', 'stockmarket_data/SCGLY.csv',
    'stockmarket_data/SCHW.csv', 'stockmarket_data/SCMWY.csv', 'stockmarket_data/SCPJ.csv', 'stockmarket_data/SCTBF.csv',
    'stockmarket_data/SDVKY.csv', 'stockmarket_data/SDXAY.csv', 'stockmarket_data/SEBYF.csv', 'stockmarket_data/SEE.csv',
    'stockmarket_data/SEIC.csv', 'stockmarket_data/SEKEY.csv', 'stockmarket_data/SEOAY.csv', 'stockmarket_data/SFOSF.csv',
    'stockmarket_data/SGBLY.csv', 'stockmarket_data/SGDBF.csv', 'stockmarket_data/SGFEF.csv', 'stockmarket_data/SGHIY.csv',
    'stockmarket_data/SGSOF.csv', 'stockmarket_data/SHASF.csv', 'stockmarket_data/SHECY.csv', 'stockmarket_data/SHG.csv',
    'stockmarket_data/SHLAF.csv', 'stockmarket_data/SHPG.csv', 'stockmarket_data/SHTDF.csv', 'stockmarket_data/SHW.csv',
    'stockmarket_data/SIELY.csv', 'stockmarket_data/SIG.csv', 'stockmarket_data/SINGY.csv', 'stockmarket_data/SIOLF.csv',
    'stockmarket_data/SIOPF.csv', 'stockmarket_data/SIVB.csv', 'stockmarket_data/SJR.csv', 'stockmarket_data/SJW.csv',
    'stockmarket_data/SKFOF.csv', 'stockmarket_data/SKHSF.csv', 'stockmarket_data/SKLKF.csv', 'stockmarket_data/SKM.csv',
    'stockmarket_data/SKSUF.csv', 'stockmarket_data/SLB.csv', 'stockmarket_data/SLF.csv', 'stockmarket_data/SLG.csv', 'stockmarket_data/SLLDY.csv', 'stockmarket_data/SMEBF.csv', 'stockmarket_data/SMFG.csv', 'stockmarket_data/SMFKY.csv',
    'stockmarket_data/SMGZY.csv', 'stockmarket_data/SMMYY.csv', 'stockmarket_data/SMTOY.csv', 'stockmarket_data/SMTUF.csv',
    'stockmarket_data/SNI.csv', 'stockmarket_data/SNMCY.csv', 'stockmarket_data/SNN.csv', 'stockmarket_data/SNV.csv',
    'stockmarket_data/SO.csv', 'stockmarket_data/SOHVF.csv', 'stockmarket_data/SOMMY.csv', 'stockmarket_data/SPB.csv',
    'stockmarket_data/SPR.csv', 'stockmarket_data/SRE.csv', 'stockmarket_data/SREDY.csv', 'stockmarket_data/SRG.csv',
    'stockmarket_data/SRGHY.csv', 'stockmarket_data/SSAAY.csv', 'stockmarket_data/SSNLF.csv', 'stockmarket_data/SSREY.csv',
    'stockmarket_data/SSUMY.csv', 'stockmarket_data/STAG.csv', 'stockmarket_data/STBFF.csv', 'stockmarket_data/STGPF.csv',
    'stockmarket_data/STJPF.csv', 'stockmarket_data/STLD.csv', 'stockmarket_data/STM.csv', 'stockmarket_data/STT.csv',
    'stockmarket_data/STWD.csv', 'stockmarket_data/STX.csv', 'stockmarket_data/STZ-B.csv', 'stockmarket_data/SU.csv',
    'stockmarket_data/SUEZY.csv', 'stockmarket_data/SUGBY.csv', 'stockmarket_data/SUHJY.csv', 'stockmarket_data/SURRY.csv',
    'stockmarket_data/SURVF.csv', 'stockmarket_data/SVJTY.csv', 'stockmarket_data/SVNDY.csv', 'stockmarket_data/SVNLF.csv',
    'stockmarket_data/SVTRF.csv', 'stockmarket_data/SVYSF.csv', 'stockmarket_data/SWGAY.csv', 'stockmarket_data/SWK.csv',
    'stockmarket_data/SWKS.csv', 'stockmarket_data/SWRBY.csv', 'stockmarket_data/SWSKF.csv', 'stockmarket_data/SWZNF.csv',
    'stockmarket_data/SXI.csv', 'stockmarket_data/SYF.csv', 'stockmarket_data/SYNT.csv', 'stockmarket_data/SZHIF.csv',
    'stockmarket_data/SZKMY.csv', 'stockmarket_data/SZLMY.csv', 'stockmarket_data/T.csv', 'stockmarket_data/TAP.csv',
    'stockmarket_data/TCLCF.csv', 'stockmarket_data/TCTZF.csv', 'stockmarket_data/TDBOF.csv', 'stockmarket_data/TDHOY.csv',
    'stockmarket_data/TEF.csv', 'stockmarket_data/TEL.csv', 'stockmarket_data/TELNY.csv', 'stockmarket_data/TEN.csv',
    'stockmarket_data/TEPCY.csv', 'stockmarket_data/TFIV.csv', 'stockmarket_data/TGOPY.csv', 'stockmarket_data/THC.csv',
    'stockmarket_data/THFF.csv', 'stockmarket_data/THLEF.csv', 'stockmarket_data/THYCF.csv', 'stockmarket_data/TIACF.csv',
    'stockmarket_data/TKAMY.csv', 'stockmarket_data/TKGSF.csv', 'stockmarket_data/TKOMY.csv', 'stockmarket_data/TLK.csv',
    'stockmarket_data/TM.csv', 'stockmarket_data/TMIX.csv', 'stockmarket_data/TMO.csv', 'stockmarket_data/TNABF.csv',
    'stockmarket_data/TOELY.csv', 'stockmarket_data/TOKUF.csv', 'stockmarket_data/TONPF.csv', 'stockmarket_data/TOWN.csv',
    'stockmarket_data/TPHIF.csv', 'stockmarket_data/TRAUF.csv', 'stockmarket_data/TRGNF.csv', 'stockmarket_data/TRGP.csv',
    'stockmarket_data/TRI.csv', 'stockmarket_data/TROW.csv', 'stockmarket_data/TRYIY.csv', 'stockmarket_data/TS.csv',
    'stockmarket_data/TSCDY.csv', 'stockmarket_data/TSCO.csv', 'stockmarket_data/TSEM.csv', 'stockmarket_data/TSLA.csv',
    'stockmarket_data/TSN.csv', 'stockmarket_data/TTGPF.csv', 'stockmarket_data/TTM.csv', 'stockmarket_data/TUIFF.csv',
    'stockmarket_data/TV.csv', 'stockmarket_data/TVFCF.csv', 'stockmarket_data/TW.csv', 'stockmarket_data/TWO.csv',
    'stockmarket_data/TWODY.csv', 'stockmarket_data/TWX.csv', 'stockmarket_data/TX.csv', 'stockmarket_data/TXN.csv',
    'stockmarket_data/TXT.csv', 'stockmarket_data/TYHOF.csv', 'stockmarket_data/TYIDY.csv', 'stockmarket_data/TZOO.csv',
    'stockmarket_data/UABK.csv', 'stockmarket_data/UAL.csv', 'stockmarket_data/UCBJY.csv', 'stockmarket_data/UGP.csv',
    'stockmarket_data/UHS.csv', 'stockmarket_data/UL.csv', 'stockmarket_data/UMICY.csv', 'stockmarket_data/UNBK.csv',
    'stockmarket_data/UNCFF.csv', 'stockmarket_data/UNH.csv', 'stockmarket_data/UNM.csv', 'stockmarket_data/UNP.csv',
    'stockmarket_data/UNPSF.csv', 'stockmarket_data/UOVEY.csv', 'stockmarket_data/UPS.csv', 'stockmarket_data/URI.csv',
    'stockmarket_data/USB.csv', 'stockmarket_data/USCS.csv', 'stockmarket_data/UUGRY.csv', 'stockmarket_data/V.csv',
    'stockmarket_data/VCISY.csv', 'stockmarket_data/VDNRF.csv', 'stockmarket_data/VEOEY.csv', 'stockmarket_data/VIP.csv',
    'stockmarket_data/VIPS.csv', 'stockmarket_data/VIVEF.csv', 'stockmarket_data/VLEEY.csv', 'stockmarket_data/VLPNF.csv',
    'stockmarket_data/VMC.csv', 'stockmarket_data/VMW.csv', 'stockmarket_data/VNDA.csv', 'stockmarket_data/VNRFY.csv',
    'stockmarket_data/VOD.csv', 'stockmarket_data/VOYA.csv', 'stockmarket_data/VPRIF.csv', 'stockmarket_data/VRSK.csv',
    'stockmarket_data/VRSN.csv', 'stockmarket_data/VTR.csv', 'stockmarket_data/VUPPF.csv', 'stockmarket_data/VWSYF.csv',
    'stockmarket_data/VZ.csv', 'stockmarket_data/WAWL.csv', 'stockmarket_data/WBA.csv', 'stockmarket_data/WCN.csv',
    'stockmarket_data/WDAY.csv', 'stockmarket_data/WDC.csv', 'stockmarket_data/WEC.csv', 'stockmarket_data/WEFIF.csv', 'stockmarket_data/WEICY.csv', 'stockmarket_data/WELPP.csv', 'stockmarket_data/WFAFY.csv', 'stockmarket_data/WHR.csv',
    'stockmarket_data/WING.csv', 'stockmarket_data/WJRYY.csv', 'stockmarket_data/WLKP.csv', 'stockmarket_data/WLMIF.csv',
    'stockmarket_data/WM.csv', 'stockmarket_data/WMT.csv', 'stockmarket_data/WNDLF.csv', 'stockmarket_data/WNGRF.csv',
    'stockmarket_data/WOLWF.csv', 'stockmarket_data/WRK.csv', 'stockmarket_data/WRTBF.csv', 'stockmarket_data/WSPOF.csv',
    'stockmarket_data/WTBCF.csv', 'stockmarket_data/WTFC.csv', 'stockmarket_data/WTKWY.csv', 'stockmarket_data/WU.csv',
    'stockmarket_data/WWNTF.csv', 'stockmarket_data/WY.csv', 'stockmarket_data/WYNN.csv', 'stockmarket_data/XEL.csv',
    'stockmarket_data/XL.csv', 'stockmarket_data/XOM.csv', 'stockmarket_data/XPO.csv', 'stockmarket_data/YACAF.csv',
    'stockmarket_data/YAMHF.csv', 'stockmarket_data/YARIY.csv', 'stockmarket_data/YATRF.csv', 'stockmarket_data/YFGSF.csv',
    'stockmarket_data/YMDAF.csv', 'stockmarket_data/YUM.csv', 'stockmarket_data/YWGRF.csv', 'stockmarket_data/ZBH.csv',
    'stockmarket_data/ZIJMF.csv', 'stockmarket_data/ZION.csv', 'stockmarket_data/ZNH.csv', 'stockmarket_data/ZSHGY.csv',
    'stockmarket_data/ZTS.csv', 'stockmarket_data/ZURVY.csv']


merged_df = pd.DataFrame()

'''
common_dates = None

# Find common dates
for file_path in files:
    df = pd.read_csv(file_path)
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
    df = df.drop(columns=['Low', 'Open', 'Volume', 'High', 'Adjusted Close'])
    df = df[df['Date'] >= '2019-12-01']
    current_dates = set(df['Date'].dt.date)
    if common_dates is None:
        common_dates = current_dates
    else:
        common_dates = common_dates.intersection(current_dates)

common_dates_list = [str(date) for date in common_dates]

'''

# Process individual files and calculate weekly means
for file_path in files:
    df2 = pd.read_csv(file_path)
    # Drop irrelevant columns
    df2 = df2.drop(columns=['Low', 'Open', 'Volume', 'High', 'Adjusted Close'])
    # Convert 'Date' to datetime format
    df2['Date'] = pd.to_datetime(df2['Date'], format='%d-%m-%Y')
    # Sort the DataFrame by the 'Date' column
    df2 = df2.sort_values(by='Date')
    # Filter the DataFrame to keep only rows with dates in the common_dates list
    #df2 = df2[df2['Date'].dt.date.astype(str).isin(common_dates_list)]
    # Calculate the mean of the 'Close' column for each week
    weekly_mean = df2.resample('W-Mon', on='Date')['Close'].mean().reset_index()
    # Concatenate the mean values to the merged DataFrame
    merged_df = pd.concat([merged_df, weekly_mean], ignore_index=True, axis=0)

# Convert 'Date' column to datetime
merged_df['Date'] = pd.to_datetime(merged_df['Date'], format='%d-%m-%Y')

# Filter rows starting from December 1, 2019, to the last date
merged_df = merged_df[merged_df['Date'] >= '2019-12-01']

merged_df = merged_df.sort_values(by='Date')

weekly_df = []

# Process each week
for i in range(len(weeks) - 1):
    start_date = weeks[i]
    end_date = weeks[i + 1]

    # Use boolean indexing to filter rows within the specified date range
    week_data = merged_df[(merged_df['Date'] >= start_date) & (merged_df['Date'] < end_date)]

    # Append the filtered rows to weekly_df
    weekly_df.append(week_data)

# Create a new DataFrame with 'Week' and 'Mean_Close' columns
result_df = pd.DataFrame(columns=['Week', 'Mean_Close'])

# Populate the new DataFrame with actual week starting from 2019-12-02 and corresponding mean values
for i, week_data in enumerate(weekly_df):
    start_date_str = (weeks[i] + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date_str = (weeks[i + 1] + timedelta(days=1)).strftime('%Y-%m-%d')
    mean_close = week_data['Close'].mean()
    result_df = pd.concat(
        [result_df, pd.DataFrame({'Week': [f"{start_date_str} - {end_date_str}"], 'Mean_Close': [mean_close]})],
        ignore_index=True)

file_path = '/Users/christiansawadogo/Desktop/Studium/Fünftes Semester/result.csv'

# Export the DataFrame to a CSV file
result_df.to_csv(file_path, index=False)
