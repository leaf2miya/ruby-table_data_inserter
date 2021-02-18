# frozen_string_literal: true

require 'bundler/setup'
require 'sequel'
require 'optparse'
require 'securerandom'
require 'digest'
# debug mode
require 'ruby_jard' if ENV['RUBY_DEBUG'] == '1'

# insert random data
class InsertRandomData
  # start point
  # @param [Array] args command line arguments
  def self.start(args = ARGV)
    new.main args
  end

  # initialize
  def initialize
    @cnt_cache = {}
    @record_cache = {}
    @ignore_tables = []
  end

  # main thread
  # @param [Array] args command arguments
  # @return [Integer] exit code
  def main(args)
    parse args
    uri, cnt = ARGV
    db = get_connection uri
    foreign_key_list, unsorted_tables = create_foreign_key_list(db)
    sorted_table = sort_table(unsorted_tables, foreign_key_list)
    # cleaning
    put_message "data cleaning has started"
    sorted_table.reverse.each { |tbl| db[tbl].delete }
    put_message "data cleaning is complete"
    # insert data per table
    sorted_table.each do |tbl|
      db.transaction { insert_data(db, tbl, cnt) }
    end
    0
  end

  # insert table data
  # @param [Sequel::Database] db Sequel object
  # @param [Symbol] tbl table name
  # @param [Integre] cnt insert count
  def insert_data(db, tbl, cnt)
    put_message "start #{tbl} table"
    schema_info = db.schema tbl
    fk_info = db.foreign_key_list tbl
    cnt.to_i.times do
      data = create_data(db, schema_info, fk_info, tbl)
      db.transaction(savepoint: true) { db[tbl].insert data }
    rescue Sequel::UniqueConstraintViolation
      # ignore error
    end
  end
  private :insert_data

  # create to insert record data
  # @param [Sequel::Database] db Sequel object
  # @param [Hash] schema_info schema information
  # @param [Hash] fk_info foreign keys information
  # @param [Symbol] tbl table name
  # @return [Hash] created record data
  def create_data(db, schema_info, fk_info, tbl) # rubocop:disable Metrics/AbcSize
    skip_keys = []
    record_data = {}
    # for foreign key column
    fk_info.each do |fk|
      data = {}
      reference_data = if fk[:table] == tbl
                         # for self-reference
                         []
                       else
                         table_cnt = get_record_count(db, fk[:table])
                         offset = table_cnt == 1 ? 0 : Random.rand(table_cnt)
                         get_column_value(db, fk[:table], fk[:key], offset)
                       end
      fk[:columns].each do |col|
        data[col] = reference_data.shift
        skip_keys << col
      end
      record_data.update data
    end
    # common data
    schema_info.each do |info|
      col = info[0]
      next if skip_keys.include? col

      record_data.update generate_data(col, info[1])
    end
    record_data
  end
  private :create_data

  # generate data by type
  # @param [Symbol] colum column name
  # @param [Hash] schema_info scehma schema_infomation
  # @return [Hash] generated data
  def generate_data(column, schema_info) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
    generated = {}
    value = case schema_info[:type]
            when :integer
              n = schema_info[:db_type].match?(/\Atinyint/) ? 127 : 2147483647
              SecureRandom.random_number(n)
            when :string
              n = schema_info[:max_length] || 1
              SecureRandom.alphanumeric(n)
            when :date
              Date.today - SecureRandom.random_number(366)
            when :datetime
              DateTime.now - SecureRandom.random_number(366)
            when :boolean
              SecureRandom.random_number(2).even?
            when :time
              Time.now - SecureRandom.random_number(24 * 60 * 60)
            when :blob
              SecureRandom.random_bytes(1024)
            else
              raise "unknown data type: #{schema_info}"
            end
    generated[column] = value
    generated
  end
  private :generate_data

  # make foreign key list
  # @param [Sequel::Database] db Sequel object
  # @return [Array] foreign key list(Hash), no use foreign keys(Array)
  def create_foreign_key_list(db)
    nouse_foreign_key_list = []
    foreign_key_list = Hash.new { |h, k| h[k] = [] }
    db.tables.each do |tbl|
      next if @ignore_tables.include? tbl

      nouse_foreign_key_list << tbl
      db.foreign_key_list(tbl).each do |fk|
        # hash key: referensed table
        # hash value: refer tables
        foreign_key_list[fk[:table]] << tbl
      end
    end
    [foreign_key_list, nouse_foreign_key_list]
  end
  private :create_foreign_key_list

  # sort tables
  # @param [Array] tables unsorted tables
  # @param [Hash] fk_list foreign key list
  # @option fk_list [Symbol] :<table name> constraint foreign key tables
  # @return [Array] sorted tables
  def sort_table(tables, fk_list)
    sorted = create_initial_sorted_data fk_list
    tables.each do |tbl|
      next if sorted.include? tbl

      foreign_keys = search_hash_element(fk_list, tbl)
      if foreign_keys.empty?
        sorted << tbl
      else
        # If you use a foreign key constraint, insert the key after the last foreign key
        pos = -1
        foreign_keys.each do |key|
          pos = sorted.index(key) if sorted.index(key) > pos
        end
        sorted.insert(pos + 1, tbl)
      end
    end
    sorted
  end
  private :sort_table

  # search element in hash
  # @param [Hash] list hash data
  # @param [Object] element search data
  # @return [Object] find data
  def search_hash_element(list, element)
    ret = []
    list.each do |key, value|
      ret << key if value.include? element
    end
    ret
  end
  private :search_hash_element

  # make a first sorted data
  #   consider constraint: foreign key
  # @param [Hash] fk_list foreign key list
  # @option fk_list [Symbol] :<table name> constraint foreign key tables
  # @param [Array] sorted sorted data
  # @return [Array] sorted data
  def create_initial_sorted_data(fk_list, sorted = [])
    copy_list = fk_list.dup
    values = fk_list.values.flatten.uniq
    fk_list.each_key do |tbl|
      unless values.include?(tbl)
        sorted << tbl
        copy_list.delete tbl
      end
    end
    if copy_list.eql?(fk_list)
      sorted += copy_list.keys
    else
      create_initial_sorted_data copy_list, sorted
    end
  end
  private :create_initial_sorted_data

  # parse arguments
  # @param [Array] args arguments
  def parse(args)
    opt = OptionParser.new do |opts|
      opts.banner = 'Usage: random_inserter.rb connect_uri count'
      opts.on('-v', 'put verbose message') { @verbose = true }
      opts.on('--ignore TBL', 'ignore insert table') do |tbl|
        @ignore_tables << tbl.to_sym
      end
    end
    opt.parse! args
    raise ArgumentError, 'argument more or less than 2' if ARGV.length != 2
    raise ArgumentError, 'argument mismatch. count more than 1' if ARGV[1].to_i < 1
  end
  private :parse

  # create db connection
  # @param [String] uri connection uri
  # @return [Sequel::Database] Sequel object
  # @raise [Sequel::Error] failed to create db connection
  def get_connection(uri)
    Sequel.connect uri
  rescue Sequel::Error => e
    raise Sequel::Error, "connection failed #{uri}: #{e.message}"
  end
  private :get_connection

  # put message
  # @param [String] msg put message
  def put_message(msg)
    puts "#{Time.now.strftime('%H:%M:%S')} #{msg}" if @verbose
  end
  private :put_message

  # get table record count
  #   1. get data from cache
  #   2, get data from table and store data in cache when has not data in cache
  # @param [Sequel::Database] db Sequel object
  # @param [Symbol] tbl table name
  # @return [Integer] record count
  def get_record_count(db, tbl)
    @cnt_cache[tbl] = db[tbl].count unless @cnt_cache.key? tbl
    @cnt_cache[tbl]
  end
  private :get_record_count

  # get random column value set
  #   1. get data from cache
  #   2, get data from table and store data in cache when has not data in cache
  # @param [Sequel::Database] db Sequel object
  # @param [Symbol] tbl table name
  # @param [Array]  columns colum name set (element type: symbol)
  # @param [Integer] offset offset value
  # @return [Array] column value set
  def get_column_value(db, tbl, columns, offset)
    access_key = Digest::MD5.hexdigest "#{tbl}#{columns.join}"
    @record_cache[access_key] = db[tbl].select_map(columns) unless @record_cache.key?(access_key)
    @record_cache[access_key][offset].dup
  end
  private :get_column_value
end

InsertRandomData.start if $PROGRAM_NAME == __FILE__
