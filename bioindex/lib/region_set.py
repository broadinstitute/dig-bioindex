import boto3
import functools
from functools import reduce
import glob
import logging
from ncls import NCLS
import pandas as pd
from timeit import default_timer as timer
import re

def time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = timer()
        result = func(*args, **kwargs)
        end = timer()
        elapsed = end - start
        logging.debug(f"{func.__name__!r} took {elapsed:.4f} secs")

        return result
    return wrapper

class Region:
  def __init__(self, chrom, start, end) :
      self.chrom = chrom
      self.start = start
      self.end = end
  def __str__(self) -> str:
      return "Region(%s, %s, %s)" % (self.chrom, self.start, self.end)

class Variant:
  def __init__(self, chrom, pos):
      self.chrom = chrom
      self.pos = pos
  def __str__(self) -> str:
      return "Variant(%s, %s)" % (self.chrom, self.pos)

"""
Allows querying large numbers of regions to see which ones contain given variants.
Provides:
  regions_containing_single()  (Variant => Set[Region])
  regions_containing()         (Iterable[Variant] => Set[Region])

Constructor params:
  dir: a path on the local FS or S3 that contains .csv files containing region data.
    The CSVs are expected to have the following format:
      - No header
      - 3+ columns, starting with CHROM, START, END
      - START and END are integers
      - CHROM is a string that's one of "1"..."22", "X", "Y", "XY", or "MT"
  delim: the delimiter used in the .csv files (default is a tab character).
"""
class RegionSet:
  def __init__(self, dir, delim = '\t'):
    self.trees_by_chrom = None
    self.delim = delim
    match = re.search('^s3://(.+?)/(.+?)$', dir)
    if match:
      self.bucketName = match.group(1)
      self.dir = match.group(2)
    else:
      self.bucketName = None
      self.dir = dir

  """
  Takes a Variant (or anything with chrom: String and pos: Int fields)
  and returns the set of regions that contain that variant.
  """
  #@time
  def regions_containing_single(self, variant):
    trees = self.__build_trees()
    
    if variant.chrom not in trees:
      raise ValueError("Unknown chromosome '%s'" % variant.chrom)

    #@time
    def search():
      return trees[variant.chrom].find_overlap(variant.pos, variant.pos + 1)
    
    return set(search())

  """
  Takes a iterable bunch of Variants (or anything with chrom: String and pos: Int fields)
  and returns the set of regions that contain any of those variants.  
  """
  #@time
  def regions_containing(self, variants):

    region_sets = (self.regions_containing_single(v) for v in variants)

    result = set()

    for rs in region_sets:
      result.update(rs)

    return result

  def __build_trees(self):
    def regions_df():
      if self.bucketName:
        return self.__regions_df_s3()
      else:
        return self.__regions_df_local()

    #@time
    def do_build():
      logging.info("Building trees")

      regions = regions_df()

      def ncls_for_chrom(chrom):
        filtered = regions[regions["chrom"] == chrom.upper()]

        tree = NCLS(filtered["start"], filtered["end"], filtered["idx"])

        logging.info("Loaded %s intervals for chrom '%s'" % (len(tree.intervals()), chrom))

        return tree

      chroms = [str(c + 1) for c in range(22)] + ["X", "Y", "XY", "MT"]

      self.trees_by_chrom = dict([(chrom, ncls_for_chrom(chrom)) for chrom in chroms])


    if self.trees_by_chrom is None:
      do_build()      

    return self.trees_by_chrom

  def __regions_df_s3(self):
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(self.bucketName)

    client = boto3.client("s3")

    def csv_s3_keys(): 
      dirs = bucket.objects.filter(Prefix=self.dir)

      for dir in dirs:
        for file in bucket.objects.filter(Prefix=dir.key):
          if(file.key.endswith('.csv')):
            yield file.key

    def read(key):
      logging.info("Reading from '%s'" % key)

      response = client.get_object(Bucket=self.bucketName, Key=key)

      status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

      if status == 200:
        return self.__read_region_df(response.get("Body"))
      else:
        return None

    regions = pd.concat((read(k) for k in csv_s3_keys()), ignore_index=True)

    regions["idx"] = range(1, len(regions) + 1)

    logging.info("Found %s total regions" % len(regions))

    return regions

  def __read_region_df(self, f):
    df = pd.read_csv(f, names=["chrom", "start", "end"], delimiter=self.delim)

    def normalize_chrom(c):
      if c == "23":
        return "X"
      elif c == "24":
        return "Y"
      elif c == "25":
        return "XY"
      elif c == "26" or c == "M":
        return "MT"
      else: 
        return c

    df["chrom"] = df["chrom"].str.upper()
    df["chrom"] = df["chrom"].apply(lambda c: normalize_chrom(c))
    df["start"] = df["start"].astype(int)
    df["end"] = df["end"].astype(int)

    return df

  def __regions_df_local(self):
    all_files = glob.glob("data/regions/csvs/*.csv")

    def read(f):
      logging.info("Reading from '%s'" % f)

      return self.__read_region_df(f)

    regions = pd.concat((read(f) for f in all_files), ignore_index=True)

    regions["idx"] = range(1, len(regions) + 1)

    logging.info("Found %s total regions" % len(regions))

    return regions
