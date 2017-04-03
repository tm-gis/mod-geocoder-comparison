#!/usr/bin/perl
#

use strict;
my %cf;
my $same=0;
my $mm=0;
my $gg=0;
my $okm=0;
my $okg=0;
my $tot=0;
my $covg=0;
my $covm = 0;
my $ext = 'hmm3';
my $f = open(F, "geogeomapbx.txt");
while (<F>) {
chomp;
my $line = $_;
print "line: $line\n";
my @a = split(/\,/,$_);
my $confi = pop @a;
if ($confi == 1 || $confi == 0.9 || $confi == 0.8 || $confi == 0.7 || $confi == 0.6 || $confi == 0.5 || $confi>0) {

} else {
$confi = '0.0';
}
print "confidence: $confi\n";

$cf{$confi}++;
next unless $a[0]>2;

my $dm = $a[-1];
my $dg = $a[-4];
$tot++;
my $whichm;
if ($dm<0.5) {
$okm++;
} else {
$whichm = 'mmmm'.$dm;
}

my $whichg;
if ($dg<0.5) {
$okg++;
} else {
if ($dg>5) {
$whichg = "AAAoffgg".$dg;
} else {
$whichg = "offgg".$dg;
} 
}

if ($dg<20) {
$covg++;
}
if ($dm<20) {
$covm++;
}

if ($dm>$dg) {
$mm++;
#print "$dm>$dg Geoc: $mm\n";
my $sc = "OFF";
if ($dg<1) {
$sc="OK";
}

print "$line,G,$sc,$whichg;$whichm\n";
} elsif ($dm == $dg) {
$same++;

} else {
my $sc = "OFF";
if ($dm<1) {
$sc="OK";
}

print "$line,M,$sc,$whichg,$whichm\n";
$gg++;
#print "$dm<$dg Mapz: $gg\n";
}

}
close F;
foreach (keys %cf) {
print $_ . " = " . $cf{$_} . "\n";
}
print "Geocoder.ca : $okg out of $tot (accurate within 500m)\n";
print "MapBox  : $okm out of $tot (accurate within 500m)\n";

print "Geocoder.ca More accurate: $mm times\n";
print "MapBox More accurate: $gg times\n";
print "The Same: $same\n";
print "Coverage:\n";
print "\t Geocoder.ca $covg out of $tot\n";
print "\t MapBox $covm out of $tot\n";
