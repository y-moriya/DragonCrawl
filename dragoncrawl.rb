# encoding: utf-8

require 'rubygems'
require 'parallel'
require 'open-uri'
require 'nokogiri'
require 'sqlite3'
require 'yaml'
require 'pp'

def loadconfig
	argfiles = ARGF.read()
	@config = YAML.load(argfiles)
end

def main
	# dbオープン
	db = SQLite3::Database.new(@config["db"])
	
	unless db.execute("SELECT tbl_name FROM sqlite_master WHERE type == 'table'").flatten.include?(@config["table"])
		db.execute(@config["createsql"].sub("_database_", @config["table"]))
		db.execute(@config["createsummarysql"].sub("_database_", @config["table"] + "_summary"))
		db.execute(insertsql(@config["table"]), {:id => 0, :name => "name", :number => 0, :star => "star", :amount => 0, :username => "username", :userid => "userid", :continent => "continent", :fromdate => "fromdate", :todate => "todate", :unitprice => 0})
	end
	
	id = db.execute(selectsql(@config["table"]))
	id = id[0][0].to_i + 1

	now = Time.now.strftime("%Y/%m/%d %H:%M")
	
	urls = (0..@config["parallels_of_degrees"]-1).map {|i| "http://hiroba.dqx.jp/sc/search/bazaar/#{@config["item_id"]}/page/#{i}" }
	results = Parallel.map(urls, :in_threads => @config["parallels_of_degrees"]) {|url|
		puts "download: #{url}"
		file = open(url, {'Cookie' => @config["cookie"]})
		puts "downloaded: #{url}"
		file
	}
	
	i = 1
	columns = 0
	sum_number = 0
	sum_amount = 0
	sum_unitprice = 0
	totalcount = 0
	results.each do |file|
		puts "start #{i}"
		doc = Nokogiri::HTML(file)
		tds = doc.css(".bazaarTableTd")
		tds.each do |td|
			name = td.css(".col11Td .strongLnk").text()
			number = extractNumber(td.css(".col12Td").text())
			star = td.css(".col13Td").text()
			amount = extractAmount(td.css(".col14Td").text())
			username = td.css(".col15Td .strongLnk").text()
			userid = extractUserid(td.css(".col15Td .strongLnk").attribute("href"))
			continent = td.css(".col16Td").text()
			fromdate = td.css(".col17Td").text()
			todate = td.css(".col18Td").text()
			unitprice = amount / number
			
			item = {:id => id, :name => name, :number => number, :star => star, :amount => amount, :username => username, 
					:userid => userid, :continent => continent, :fromdate => fromdate, :todate => todate, :unitprice => unitprice,
					:created_at => now }
			
			db.execute(insertsql(@config["table"]), item)
			id += 1
			columns += 1
			sum_number += number
			sum_amount += amount
			sum_unitprice += unitprice
		end
		totalcount = extractTotal(doc.css(".searchResult").text())
		i += 1
	end
	
	avg_number = sum_number.to_f / columns.to_f
	avg_amount = sum_amount.to_f / columns.to_f
	avg_unitprice = sum_unitprice.to_f / columns.to_f
	
	summary = {:created_at => now, :name => @config["item_name"], :number => avg_number, :amount => avg_amount, :unitprice => avg_unitprice, :totalcount => totalcount}
	db.execute(insertsummarysql(@config["table"]), summary)
	
	db.close
end

def extractAmount(str)
	if (/([0-9]+) G/ =~ str)
		result = $1
		return result.to_i
	end
	
	return 0
end

def extractTotal(str)
	if (/全([0-9]+)/ =~ str)
		result = $1
		return result.to_i
	end
	
	return 0
end

def extractUserid(str)
	if (/([0-9]+)/ =~ str)
		return $1
	end
	
	return "0"
end

def extractNumber(str)
	if (/([0-9]+)/ =~ str)
		result = $1
		return result.to_i
	end
	
	return 0
end

def insertsql(table)
	return "insert into " + table + " values(:id, :name, :number, :star, :amount, :username, :userid, :continent, :fromdate, :todate, :unitprice, :created_at)"
end

def selectsql(table)
	return "select max(id) from " + table
end

def insertsummarysql(table)
	return "insert into " + table + "_summary values(:created_at, :name, :number, :amount, :unitprice, :totalcount)"
end

loadconfig
main
