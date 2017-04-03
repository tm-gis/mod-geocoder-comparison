#!/usr/bin/perl -w
# diary-link-checker - check links from diary page
use strict;
use Modern::Perl;
use Mojo::DOM;
use Mojo::URL;
use Selenium::Remote::Driver;
 use XML::Simple qw(:strict);
use GD::Image;
use Image::Magick;
use Try::Tiny;
use Data::Dumper;

use JSON::Parse 'parse_json';

exit if get_load_average() > 15;
my $sai=0;

sub get_load_average {
  open(LOAD, "/proc/loadavg") or die "Unable to get server load \n";
  my $load_avg = <LOAD>;
  close LOAD;
  my ( $one_min_avg ) = split /\s/, $load_avg;
  return $one_min_avg;
}

use Sys::RunAlone;

my %err;

use DBI;
my $dbh;
$dbh = DBI->connect("DBI:mysql:host=localhost;mysql_connect_timeout=2;database=dinehere", "whois", "this",
                            { PrintError => 1, RaiseError => 1}) ;
my $ok=0;
my $nok=0;
my $tot=0;
my $stt = $dbh->prepare("select address,lat,lon from gcrawl where address like '%OR%' and address not like '%qc%' and address is not null and lat>0 and lon<0 order by time desc");
$stt->execute;
while (my ($adr,$lat,$lon) = $stt->fetchrow_array()) {
next unless $adr =~ /\bOR\b/;

#$adr = "300 Pearl Street, Burlington, VT";
$tot++;
exit if $tot>10000;

$adr =~ s/\#(\d+)//g;
$adr =~ s/\#(\w)//g;
$adr =~ s/ /\+/g;
$adr =~ s/\#//g;
next if $adr =~ /http/i;
#Foursquare+https://foursquare.com
next if $adr =~ /\//i;
next if $adr =~ /www/i;
next if $adr =~ /Phone:/i;
my $locatiuon = $adr;
$locatiuon =~ s/\+/ /g;

my $gurl = "http://geocoder.ca/?locate=$adr&auth=404860428005310765659x1&geoit=xml&standard=1";
my $gurlg = $gurl;
$gurl = "http://geocoder.ca/$adr?auth=404860428005310765659x1&geoit=xml&standard=1&breakisusa=1";

my ($la,$lo);
my ($mla,$mlo);
my $confi = '0.0000';
my $response = `curl "$gurl"`;
print "now get $gurl $response\n";
if ($response =~ /<confidence>(.*)<\/confidence>/) {
$confi = $1;
}

if ($response =~ /<latt>(\d{2,3})\.(\d+)<\/latt>/) {
$la = $1 . '.' . $2;
if ($response =~ /<longt>\-(\d{2,3})\.(\d+)<\/longt>/) {
$lo = '-'.$1 . '.' . $2;
}
$ok++;
} else {
$nok++;
print "$nok: not found on geo: $adr,$lat,$lon\n";
}

$gurl = "http://geocoder.ca/?locate=$adr&geoit=xml&standard=1";
#$gurl = "http://132874663939SG.geolytica.com/?locate=$adr&geoit=xml&standard=1";
#$gurl = "http://geocoder.ca/$adr?auth=404860428005310765659x1&geoit=xml&standard=1";
#https://api.mapbox.com/geocoding/v5/mapbox.places/532%20golden%20sedge%20way,%20ottawa,%20on.json?access_token=pk.eyJ1IjoiZXJ1Y2kiLCJhIjoiODg0ZDgxNGFhYzBlODlkZjA0MDAxOTRhNzI0YzRiMTQifQ.5MhopoZ7LaBl6Scyz9rFBA

$gurl = 'https://api.mapbox.com/geocoding/v5/mapbox.places/'.$adr.'.json?access_token=pk.eyJ1IjoiZXJ1Y2kiLCJhIjoiODg0ZDgxNGFhYzBlODlkZjA0MDAxOTRhNzI0YzRiMTQifQ.5MhopoZ7LaBl6Scyz9rFBA';
my $response = `curl "$gurl"`;
print "now get $gurl $response\n";

my $mapb = parse_json($response);

foreach (keys %$mapb) {
print $_ . " => " . $mapb->{$_} . "\n";

}
my $feat = $mapb->{features};
my $maxrel = 0;
foreach (@$feat) {
	my $hr = $_;
if ($hr->{relevance}>$maxrel) {
$maxrel = $hr->{relevance};
my $geom = $hr->{geometry};
foreach (keys %$geom) {
print "Geometry: " . $_ . " => " . $geom->{$_} . "\n";
}
my $center = $hr->{center};
foreach (@$center) {
print "Center: " . $_ . "\n";
if ($_ < 0) {
$mlo = $_;
} else {
$mla = $_;
}

}


	foreach (keys %$hr) {
		print "Feature: ." . $_ . "=>" . $hr->{$_} . "\n";
	}
print "Now: ($mla,$mlo)\n";
}
}
=cut
Feature: .relevance=>0.782714285714286
Feature: .place_name=>1580 Centinela Ave, Inglewood, California 90302, United States
Feature: .center=>ARRAY(0x1e03e00)
Feature: .properties=>HASH(0x1e1da30)
Feature: .text=>Centinela Ave
Feature: .bbox=>ARRAY(0x1e1da00)
Feature: .context=>ARRAY(0x1e03b90)
Feature: .address=>1580
Feature: .geometry=>HASH(0x1e03cb0)
Feature: .id=>address.11787847752000800

features => ARRAY(0x20b2c70)
query => ARRAY(0x20b2d48)
attribution => NOTICE: © 2016 Mapbox and its suppliers. All rights reserved. Use of this data is subject to the Mapbox Terms of Service (https://www.mapbox.com/about/maps/). This response and the information it contains may not be retained.
type => FeatureCollection

=cut

=cut
exit;
if ($response =~ /<latt>(\d{2,3})\.(\d+)<\/latt>/) {
$mla = $1 . '.' . $2;
if ($response =~ /<longt>\-(\d{2,3})\.(\d+)<\/longt>/) {
$mlo = '-'.$1 . '.' . $2;
}
#$ok++;
} else {
#$nok++;
print "$nok: not found on old geo: $adr,$lat,$lon\n";
}
=cut

#($mla,$mlo) = ($la,$lo);
print "ok: $ok / not ok: $nok: " . $response . "\n";
my $dist = distance($la,$lo,$lat,$lon);
my $distm = distance($mla,$mlo,$lat,$lon);
print "stats: mapbox($mla,$mlo) G: ($adr,$lat,$lon) versus: Geo: ($la,$lo) dist: $dist\n";

if ($dist > 0.2) {
print "slight problem geo($la,$lo) dist: $dist $adr\n";
$err{zero}++;
}

if ($dist > 1) {
print "problem geo($la,$lo) dist: $dist $adr\n";
$err{one}++;
}

if ($dist>10) {
$err{two}++;
print "big problem geo($la,$lo) dist: $dist $adr\n";
}

my $f = open(RR, ">>geogeomapbx.txt");
print RR "$lat,$lon,$adr,$la,$lo,$dist,$mla,$mlo,$distm,$confi\n";
close RR;
sleep(rand(10));

#exit;
}
$stt->finish;
foreach (keys %err) {
print $_ . " - " . $err{$_} . "\n";
}

exit;


sub lookupmain {
        use Socket;
        use constant TIMEOUT => 20;
        $SIG{ALRM} = sub {return "timeout"};
        my %CACHE;
        my $snapurl = shift;
        my $fname = shift;
my @h = eval <<'END';
alarm(TIMEOUT);
my @i = &main($snapurl,$fname);
alarm(0);
@i;
END
        $CACHE{$fname} = $h[0] || undef;
        return $CACHE{$fname};
}
sub main {
        my $snapurl = shift;
        my $fname = shift;
my $res;
try {
        local $SIG{ALRM} = sub { die "alarm\n" };
        alarm 20;
        $res = `nohup /var/www/pdfmenus/p/phantomjs-2.0.0/bin/phantomjs --webdriver=9997 &`;
        alarm 0;
}
catch {
        die $_ unless $_ eq "alarm\n";
        print "timed out\n";
};

        return $res;
}


sub _fetch_page {
my $url = shift;
use Try::Tiny;

my $x = `ps -ef|grep "webdriver=9997"`;
$x =~ s/\W//g;
print $x;


try {
print "try\n";
        local $SIG{ALRM} = sub { die "alarm\n" };
        alarm 24;

unless ($x =~ /phantom/i) {
`killall phantomjs`;
print "restart phantom\n";
&lookupmain($x,$x);
}
        my $driver = new Selenium::Remote::Driver('remote_server_addr' => 'localhost',
                                             'port' => '9997',
                                             'browser_name'       => 'Mozilla/5.0 (iPad; CPU OS 8_1_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B466 Safari/600.1.4',
                                             'platform'           => 'Macintosh',
                                                'host' => '108.163.139.42');
        $driver->get($url);
        my $dom = Mojo::DOM->new( $driver->get_page_source() );
        $driver->quit();
print "return\n";
        return $dom;

        alarm 0;
}
catch {
`killall phantomjs`;
print "timed out\n";
die $_ unless $_ eq "alarm\n";        
print "timed out\n";
};


}



sub absolutize {
  my($url, $base) = @_;
  use URI;
  return URI->new_abs($url, $base)->canonical;
}


sub check_url {
print "check $_[0], $_[1], $_[2]\n";
my $dbh = $_[1];
my $storeid=$_[2];

use Modern::Perl;
use Mojo::DOM;
use Mojo::URL;
use Selenium::Remote::Driver;
 use XML::Simple qw(:strict);
use GD::Image;
use Image::Magick;


use HTML::TokeParser;
  # A temporary placeholder...
  #print "I should check $_[0]\n" if $_[0] =~ /menu/i;
my $url = $_[0];

return unless $url =~ /^http/i;

return if $url =~ /.css$/i;
return if $url =~ /.js$/i;

print "checkiin $_[0]\n";

if ($_[0] =~ /menu/i) {
my $urlm = $_[0];

print "is menu $_[0]\n";

my $altext;
print "get menu: $urlm\n";


my $altext;
eval {
my $result = _fetch_page($urlm);
my $dom2 = Mojo::DOM->new($result);
$altext = $dom2;
};


#$dom2->all_text;

#print "parsed: $altext\n";

my $lengtht = length($altext);
print "menu is $lengtht long\n";
$lengtht += 11;
my $altextq = $dbh->quote($altext);
my $urlq = $dbh->quote($urlm);
print "insert into menurloa (storeid,menurl,length,menu) values ('$storeid',$urlq,$lengtht,$altextq)\n";
my $dang = $dbh->do("delete from menurloa where storeid='$storeid' and menurl=$urlq");
my $dong = $dbh->do("insert into menurloa (storeid,menurl,length,menu) values ('$storeid',$urlq,$lengtht,$altextq)");
#my $dung = $dbh->do("delete from pdfmenus where storeid='$storeid'");



}

if ($_[0] =~ /coupon/i) {
my $urlm = $_[0];
#my $res = `curl -I $url`;

=cut
if ($res =~ /200 OK/i) {
} else {
next;
}
=cut

my $altext;
my $altext;
eval {
my $result = _fetch_page($urlm);
my $dom2 = Mojo::DOM->new($result);
$altext = $dom2;
};


=cut
my $result = _fetch_page($url);
my $dom = Mojo::DOM->new($result);
my $stream = HTML::TokeParser->new( \$dom );
while (my $token = $stream->get_token) {
my $text = $stream->get_text( );
$altext .=" " . $text;
}
=cut

my $lengtht = length($altext);
my $altextq = $dbh->quote($altext);
my $urlq = $dbh->quote($url);
my $dang = $dbh->do("delete from couponrloa where storeid='$storeid' and menurl=$urlq");
my $dong = $dbh->do("insert into couponrloa (storeid,menurl,length,menu) values ('$storeid',$urlq,$lengtht,$altextq)");
}


if ($_[0] =~ /contact/i) {
my $urlm = $_[0];
=cut
my $res = `curl -I $url`;
if ($res =~ /200 OK/i) {
} else {
next;
}
=cut

my $altext;

my $altext;
eval {
my $result = _fetch_page($urlm);
my $dom2 = Mojo::DOM->new($result);
$altext = $dom2;
};

=cut
my $result = _fetch_page($url);
my $dom = Mojo::DOM->new($result);

my $stream = HTML::TokeParser->new( \$dom );
while (my $token = $stream->get_token) {
my $text = $stream->get_text( );
$altext .=" " . $text;
}
=cut

&grabemail($altext,$dbh,$storeid);
}



}

sub grabemail {
my $result = shift;
my $dbh = shift;
my $sid = shift;
use Email::Find;
use Email::Valid;
  my $finder = Email::Find->new(sub {
                                    my($email, $orig_email) = @_;
                                    print "Found ".$email->format."\n";
my $ef = $email->format;
my $emalq = $dbh->quote($email->format);
print "and $ef\n";
print "insert into emails (storeid,email) values ($sid,$emalq)\n";
my $di = $dbh->do("insert into emails (storeid,email) values ($sid,$emalq)");
                                        return ($orig_email,$email->format);
                                });
 my ($x,$eml) =  $finder->find(\$result);

}
      

sub distance {
    my ($lat1, $lon1, $lat2, $lon2) = @_;
    my $theta = $lon1 - $lon2;
    my $dist = sin(deg2rad($lat1)) * sin(deg2rad($lat2)) + cos(deg2rad($lat1)) * cos(deg2rad($lat2)) * cos(deg2rad($theta));
    $dist  = acos($dist);
    $dist = rad2deg($dist);
    $dist = $dist * 111.18957696; #in kilometres
    $dist = sprintf("%.3f",$dist);
    return ($dist);
}
sub acos {
    my ($rad) = @_;
    my $ret;
    if ($rad eq "1") {
        $ret = atan2(0,1);
    } else {
        $ret = atan2(sqrt(1 - $rad**2), $rad);
    }
    return $ret;
}
sub deg2rad {
    my ($deg) = @_;
    my $pi = 3.14159265;
    return ($deg * $pi / 180);
}
sub rad2deg {
    my ($rad) = @_;
    my $pi = 3.14159265;
    return ($rad * 180 / $pi);
}





__END__
 
