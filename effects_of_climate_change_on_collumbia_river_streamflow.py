### Written by Kateryna Gomozova and Kevin Felt

import sys
import os
import urllib as url
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
import datetime as dt
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import collections
import operator
import zipfile
from scipy.integrate import *
from scipy.interpolate import *

os.chdir(os.path.expanduser("~"))
if sys.platform.startswith("win"):
    div = '\\'
elif sys.platform.startswith("darwin"):
    div = '/'

path = os.getcwd() + div + 'streamflow_data'
if os.path.exists(path) == False:
    os.mkdir(path)

###########
###Tests###
###########
def test_peak_streamflow():
    td1 = {'01': 120.5,'02': 10.2,'03': 1453.78,'04': 232,'05': 425.3, '06': 53.836,
    '07': 363.7984,'08': 209.27, '09': 155.24,'10': 934.257,'11': 1445.094,
    '12': 1277.291}
    assert (peak_streamflow(td1)) == '03'
    
    
    td2 = {'01': 12,'02': 25,'03': 45,'04': 675,'05': 675.01, '06': 53.836,
    '07': 209.277,'08': 346.75, '09': 253.24,'10': 47.257,'11': 12.094,
    '12': 345.291}
    assert (peak_streamflow(td2)) == '05'


#################
### Functions ###
#################
def control_point_streamflow_data(gcm, rcp, downscaling_method, hydrologic_model, control_point_code, streamflow_or_biascorrected):
    '''
    Given strings of desired gcm, rcp, downscaling method, hydrologic model,
    and control point code, downloads the streamflow data for control point from
    the internet and returns a filepath to the csv file that matches the model parameters
    '''
    print "Streamflow data will be saved to", path
    retrieve = requests.get('http://hydro.washington.edu/CRCC/site_specific/' + control_point_code)
    soup = BS(retrieve.content, 'html.parser')
    data_url = str(soup.find_all('a', {'class': 'raw'})[1]['href'])
    id = data_url[data_url.index("id=") + 3 :]
    filepath = path + div + control_point_code + '.zip'
    download_file_from_google_drive("https://docs.google.com/uc?export=download", id, filepath)
    return extract_desired_data_file(gcm, rcp, downscaling_method, hydrologic_model, control_point_code, filepath, streamflow_or_biascorrected)


def extract_desired_data_file(gcm, rcp, downscaling_method, hydrologic_model, control_point_code, filepath, streamflow_or_biascorrected):
    '''
    Helps control_point_streamflow_data function
    '''
    zip_ref = zipfile.ZipFile(filepath, 'r')
    zip_ref.extractall(path)
    zip_ref.close()
    if os.path.exists(path + div + control_point_code + div + streamflow_or_biascorrected):
        files = os.listdir(path + div + control_point_code + div + streamflow_or_biascorrected)
        filename_substrings = [gcm, rcp, downscaling_method, hydrologic_model, control_point_code]
        desired_file = ''
        for file_name in files:
            substrings_found = 0
            for substring in filename_substrings:
                if substring in file_name:
                    substrings_found += 1
            if substrings_found == 5:
                desired_file = file_name
        desired_file_path = path + div + control_point_code + div + streamflow_or_biascorrected + div + desired_file
        return desired_file_path
    else:
        print '### ' + path + div + control_point_code + div + streamflow_or_biascorrected + " does not exists for this control point. Try another option for parameter streamflow_or_biascorrected. ###"
        return None

def download_file_from_google_drive(url, id, filepath):
    '''
    Downloads data from google drive location for use in extract_desired_data_file
    '''
    if os.path.exists(filepath):
        print "File already exists!"
    else:
        print "Downloading files..."
        session = requests.Session()
        response = session.get(url, params = {'id':id}, stream=True)

        token = None
        for k, v in response.cookies.items():
            if k.startswith("download_warning"):
                token = v
                break
        # found download token
        if token:
            response = session.get(url, params={'id':id, 'confirm':token}, stream=True)

        CHUNK = 1024 * 32
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(CHUNK):
                if chunk:
                    f.write(chunk)

        print "Download complete!"


def read_the_data(csv_filepath):
  return data_file


def average_streamflow(start_year, end_year, csv_filepath, streamflow_or_biascorrected):
      """
      Computes the monthly streamflow values averaged
      for a period between the years passed as parameters.

      Parameters:
          start_year: type int, indicates beginning of the period
          end_year: type int, indicates end of the period

      Return:
          average_sreamflow_dict: type dict(), represents the
          monthly streamflow values averaged for a period between
          the years passed as parameters. Dictionary is returned in
          the format: {"01": value, "02": value...}, where the keys
          are of type str, and values - float.
      """
      data = pd.read_csv(csv_filepath, header=38)
      data2 = collections.OrderedDict()
      for t in data.itertuples():
          rowd = dt.datetime.strptime(t.date, '%Y-%m-%d')
          if start_year < rowd.year and rowd.year < end_year:
              my_date = dt.datetime(1900, rowd.month, 1)
              fmy_date = dt.datetime.strftime(my_date, '%m')
              if streamflow_or_biascorrected == 'streamflow':
                  if data2.get(fmy_date, None) == None:
                      data2[fmy_date] = [t.streamflow]
                  else:
                      data2[fmy_date].append(t.streamflow)
              else:
                  if data2.get(fmy_date, None) == None:
                      data2[fmy_date] = [t.biascorrected_streamflow]
                  else:
                      data2[fmy_date].append(t.biascorrected_streamflow)

      average_sreamflow_dict = {}
      for m in data2.keys():
          average_sreamflow_dict[m] = np.mean(data2[m])
      return average_sreamflow_dict

def historic_1970_1999(csv_filepath, streamflow_or_biascorrected):
          """
          Calls average_streamflow() to produce a dictionary of average
          streamflow values for historic period, identified as a period
          from 1970 to 1999
          """
          return average_streamflow(1970,1999, csv_filepath, streamflow_or_biascorrected)

def future_2070_2099(csv_filepath, streamflow_or_biascorrected):
          """
          Calls average_streamflow() to produce a dictionary of average
          streamflow values for historic period, identified as a period
          from 2070 to 2099
          """
          return average_streamflow(2070,2099, csv_filepath, streamflow_or_biascorrected)


def print_average_streamflow_values(dictionary):
              """
              Prints the dictionary of average_streamflow_values
              """
              for key,value in dictionary.items():
                  print "The average streamflow value for month", key, "equals", round(value, 2), "cfs"


def print_historic_average_streamflow_values(csv_filepath, streamflow_or_biascorrected):
              """
              Calls print_average_streamflow_values to print the
              dictionary of average_streamflow_values for historic period
              """
              print "Streamflow data for historic period:"
              return print_average_streamflow_values(historic_1970_1999(csv_filepath, streamflow_or_biascorrected))


def print_future_average_streamflow_values(csv_filepath, streamflow_or_biascorrected):
              """
              Calls print_average_streamflow_values to print the
              dictionary of average_streamflow_values for future period
              """
              print "Streamflow data for future period:"
              return print_average_streamflow_values(future_2070_2099(csv_filepath, streamflow_or_biascorrected))


def plot_average_streamflow(csv_filepath, streamflow_or_biascorrected, control_point_code):
              """
              Produces a plot of the hydrographs for historic and future periods.
              Function returns None.
              """
              plt.clf()
              plt.plot(*zip(*sorted(historic_1970_1999(csv_filepath, streamflow_or_biascorrected).items())), label = '1970-1990 Avg.')
              plt.plot(*zip(*sorted(future_2070_2099(csv_filepath, streamflow_or_biascorrected).items())), label = '2070-2099 Avg.')
              plt.title("Hydrograph for " + control_point_code)
              plt.xlabel("Months")
              plt.ylabel("Streamflow, cfs")
              plt.legend()
              plt.savefig(path + div + 'Hydrograph' + control_point_code + '.png')
              plt.show()


def average_annual_streamflow_change(csv_filepath, streamflow_or_biascorrected):
              """
              Computes the difference between the historic and future annual
              average streamflow values.

              Return:
                  annual_streamflow_difference: type float
              """
              annual_historic = sum(historic_1970_1999(csv_filepath, streamflow_or_biascorrected).values())
              annual_future = sum(future_2070_2099(csv_filepath, streamflow_or_biascorrected).values())
              annual_streamflow_difference = (annual_future - annual_historic)
              return float(format(annual_streamflow_difference,'.2f'))

def average_monthly_streamflow_change(month, csv_filepath, streamflow_or_biascorrected):
              """
              Computes the difference between the historic and future average
              streamflow values for a specific month.

              Parameters:
                  month: type str, numerical representation of the month as follows:
                  oct = "10", mar = "03", etc.

              Return:
                  annual_streamflow_difference: type float
              """
              monthly_historic = historic_1970_1999(csv_filepath, streamflow_or_biascorrected)[month]
              monthly_future = future_2070_2099(csv_filepath, streamflow_or_biascorrected)[month]
              monthly_streamflow_difference = monthly_future - monthly_historic
              return float(format(monthly_streamflow_difference,'.2f'))

def peak_streamflow(dictionary):
              """
              Returns the month, in which the max value of the streamflow was observed.
              """
              return max(dictionary.iteritems(), key=operator.itemgetter(1))[0]

def peak_streamflow_historic(csv_filepath, streamflow_or_biascorrected):
              """
              Calls peak_streamflow to determine the month in the historic period,
              when the max value of the streamflow was obsered.
              """
              return peak_streamflow(historic_1970_1999(csv_filepath, streamflow_or_biascorrected))

def peak_streamflow_future(csv_filepath, streamflow_or_biascorrected):
              """
              Calls peak_streamflow to determine the month in the future period,
              when the max value of the streamflow was obsered.
              """
              return peak_streamflow(future_2070_2099(csv_filepath, streamflow_or_biascorrected))

def rain_snow_dominated_area(csv_filepath, streamflow_or_biascorrected):
              """
              Compares the month of the max streamflow for historic and future
              periods, and prints a message whether the control point is located
              in rain or mixed snow-rain dominated area.
              """
              message1 = "rain dominated area"
              message2 = "mixed snow-rain dominated area"
              if peak_streamflow_future(csv_filepath, streamflow_or_biascorrected) == peak_streamflow_historic(csv_filepath, streamflow_or_biascorrected):
                  return message1
              else:
                  return message2


    
#######################################################################################
### The code in this function is executed when this file is run as a Python program ###
#######################################################################################
def main():

  """
  Main function, executed when streamflow_project.py is run as a Python script.
  """
  #printout of average monthly streamflow:
  control_point_code = raw_input(" Which control point do you want? (options: AUGW, AMRW, ASHNO, ASOTI, MER," 
                                 'ALB, ALF, ALD,  ANA, APP, BEAVE, BITDA, BITDA, BLADA, BLACK, BULWA, SAN,' 
                                 'BULLY, BUM, BLKI, BLFI,  BSHI, BLU, ARKI, LUCI, BIGI, BDDI, BOMI, PARI,'  
                                 'GAINTWSP, BFE, BON, BDY, BOX, BRI, BRUNEAUR, BFKY, BURO, CALAP, CHEGR, CHEWU,' 
                                 'CISPU, CLKTH, CLALO, CLABE, CLADR, CLAGA, CLAPL, CLE, CLERI, GOSO, CLKEN, DONAL,' 
                                 'CRNIC, CRVAN, COLKE, COWCA, COWLI, COWKO, CRAIR, CRABC, CRAMO, CREO, CROO, CAB,' 
                                 'CAR, CED, CHE, CHL, CHJ, COE, CFM, COR, COT, CGR, CS2, BENO, CRAO, WICO, REREG,' 
                                 'DEBO, CULO, MADRAS, DUNBB, DUNDU, DEDI, DET, DEX, DIA, DOR, DCD, DWR, BITCO,' 
                                 'ELKFE, ELKNA, ELWPO, ENTAR, ENITA, LIBFI, FLATH, FLINT, FORDI, FAL, CHEI, FALI,'
                                 'FRN, FLTI, FOS, GOLDS, GRANB, GAL, GOR, GCL, WEN, GRAO, GRSY, GPR, HAH, AUB, GREY,' 
                                 'GVZN, HANGM, HOHFO, HCD, ANTI, HFAI, ISLI, REXI, HENI, HCR, HRJW, HOD, ARD, HGH,' 
                                 'ILLE, IMNAH, IHR, JOCDI, JOHND, JOHNR, JOHNS, JCKY, JDA, KAC, KALAR, KEE, KETFE,' 
                                 'KETWE, KICHO, SUMMI, GLENW, LEONI, KOOTE, KER, KET, KLC, LARMA, LEWCO, LISPO,' 
                                 'BLAGA, LAPO, LOCLO, MNRO, LUCKI, LAG, LEA, LIB, LIM, LGS, KLICK, LLK, LOS, SHA,' 
                                 'LCF, LWG, LMN, MALAB, VIDO, METGO, METTW, METWI, METPA, METOLIUS, WILNF, FLAWE,' 
                                 'SALYP, SALMF, JASO, MISSI, MOLAL, MOREA, WOODGOOD, MALHEUR, MAY, MCN, MCD, MFPI,' 
                                 'MON, MORI, MOS, MUC, CLFW, NACW, NEVAD, COEUR, CSCI, PAYI, PABI, NPBI, NFSTI, NOSAN,' 
                                 'NOOFE, FLANF, JOHNF, MALNF, BLADR, MEHO, NIN, NFK, NOX, OCHO, OKFAL, OKAPE, OKANA,' 
                                 'OKAOR, OKANO, OAK, OKA, ORO, OWY, ROMO, OXB, PASAY, PINEC, POWDN, PRIPR, PAK, PLS,' 
                                 'EMMI, HRSI, PLEI, PRPI, PEL, PFL, PRRO, PSL, PRD, PUY, POT, QUECL, QUIQU, RIM, ROCCR,' 
                                 'ROCHI, ROCKC, ROPIT, RYGO, RVC, RML, RIS, RRH, EGLO, ROS, ROU, SALSA, SALSM, SANRE,' 
                                 'JFFO, SATSA, SAUKR, SELLO, SFBOI, ANDI, SILSP, SIMOR, SIMPR, SIMHE, SIMNI, SKAMO, SKOPO,' 
                                 'SKYGB, SLOCR, CJSTR, SNOMO, SFNI, FLATW, WTLO, YAMMC, SPILL, JOECA, STEHE, STILL, SWANR,' 
                                 'SLM, SALY, SKC, SMH, BRN, SKHI, LORI, MILI, SRMO, AMFI, SNYI, JKSY, SNKBLWLSALMON, BFTI,' 
                                 'SNAI, BUHL, HEII, PALI, KIMI, MINI, SWAI, SHYI, WEII, PRLI, SPD, STREG, SUV, SWF, SVN,' 
                                 'THOMP, TIECH, TOBAC, TOUCH, TOUTL, TULIN, TUCAN, TULPR, TUMO, TWISP, DGGI, TRX, TEAI,' 
                                 'TDA, TOM, TMY, TRB, TRY, UMAMC, UMATI, UBK, VERNO, VAN, WARMS, WENMO, WENPE, WENPL,' 
                                 'BITWF, SANPO, WESKE, WHYCHUS, ALBO, EUGO, HARO, WILLA, WILPO, WINDR, WWA, WAV, WAN,' 
                                 'WAT, WEISERR, WEL, WHB, WHT, WRB, MUD, WHS, WFWI, RIRI, WHM, WYD, YAKT, YUMW, EASW,' 
                                 'YGVW, KIOW, YAKMA, UMTW, PARW, YAK, YAL) ')
  gcm = raw_input(" Which general circulation model do you want? (options: CanESM2, CNRM-CM5, MIROC5, HadGEM-CC, ISPL-CM5A-MR, HadGEM2-ES, inmcm4, GFDL-ESM2M, CCSM4, CSIRO-Mk3-6-0) ")
  rcp = raw_input(" Which representative concentration pathway do you want? (options: RCP45, RCP85) ")
  downscaling_method = raw_input(" Which downscaling method do you want? (options: BCSD, MACA, DYNAMICAL) ")
  hydrologic_model = raw_input(" Which hydrologic model do you want? (options: VIC_P1, VIC_P2, VIC_P3, PRMS_P1) ")
  streamflow_or_biascorrected = raw_input(" Biascorrected streamflow or non-bias corrected streamflow? (options: streamflow, biascorrected_streamflow) ")
  print
  csv_filepath = control_point_streamflow_data(gcm, rcp, downscaling_method, hydrologic_model, control_point_code, streamflow_or_biascorrected)
  print_historic_average_streamflow_values(csv_filepath, streamflow_or_biascorrected)
  print ""
  print_future_average_streamflow_values(csv_filepath, streamflow_or_biascorrected)
  print ""

  change_in_annual_streamflow = average_annual_streamflow_change(csv_filepath, streamflow_or_biascorrected)
  print "Change in annual streamflow between historic and future periods equals", change_in_annual_streamflow, "cfs"
  print ""

  plot_average_streamflow(csv_filepath, streamflow_or_biascorrected, control_point_code)

  october_streamflow_difference = average_monthly_streamflow_change('10', csv_filepath, streamflow_or_biascorrected)
  june_streamflow_difference = average_monthly_streamflow_change('06', csv_filepath, streamflow_or_biascorrected)
  print "Change in October streamflow between historic and future periods equals", october_streamflow_difference, "cfs"
  print "Change in June streamflow between historic and future periods equals", june_streamflow_difference, "cfs"
  print ""

  ppt_domination = rain_snow_dominated_area(csv_filepath, streamflow_or_biascorrected)
  print "Control point is located in", ppt_domination



if __name__ == "__main__":

    try:
        main()
    except Exception as error:
        print "### If connected to UW network, must connect to a VPN to avoid a ConnectionError :( ###"
