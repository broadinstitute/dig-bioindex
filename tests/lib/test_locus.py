from bioindex.lib.locus import Locus, SNPLocus, RegionLocus, parse_locus_builder


def test_snp_locus_default_step_unchanged():
    # 45123 // 20000 * 20000 == 40000
    assert list(SNPLocus("1", 45123).loci()) == [("1", 40000)]


def test_snp_locus_custom_step():
    # 45123 // 250 * 250 == 45000
    assert list(SNPLocus("1", 45123, step=250).loci()) == [("1", 45000)]


def test_region_locus_default_step():
    assert list(RegionLocus("1", 10000, 50000).loci()) == [("1", 0), ("1", 20000), ("1", 40000)]


def test_region_locus_custom_step_buckets():
    # 45100..45600 at step 250 -> buckets 45000, 45250, 45500
    assert list(RegionLocus("1", 45100, 45600, step=250).loci()) == [("1", 45000), ("1", 45250), ("1", 45500)]


def test_parse_locus_builder_template_bakes_step():
    builder, cols = parse_locus_builder("varId=$chr:$pos", step=250)
    assert cols == ("varId",)
    assert list(builder("1:45123:A:G").loci()) == [("1", 45000)]


def test_parse_locus_builder_column_bakes_step():
    builder, cols = parse_locus_builder("chromosome:position", step=250)
    assert cols == ("chromosome", "position", None)
    assert list(builder("1", 45123).loci()) == [("1", 45000)]


def test_parse_locus_builder_default_step_is_20000():
    builder, _ = parse_locus_builder("varId=$chr:$pos")
    assert list(builder("1:45123:A:G").loci()) == [("1", 40000)]
