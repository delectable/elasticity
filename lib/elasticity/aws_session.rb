module Elasticity

  class MissingKeyError < StandardError;
  end
  class MissingRegionError < StandardError;
  end

  class AwsSession

    attr_reader :access_key
    attr_reader :secret_key
    attr_reader :host
    attr_reader :protocol
    attr_reader :region

    # Supported values for options:
    #  :region - AWS region (e.g. us-west-1)
    #  :secure - true or false, default true.
    def initialize(access=nil, secret=nil, options={})
      # There is a cryptic error if this isn't set
      if options.has_key?(:region) && options[:region] == nil
        raise MissingRegionError, 'A valid :region is required to connect to EMR'
      end
      options[:region] = 'us-east-1' unless options[:region]
      @region = options[:region]

      @access_key = get_access_key(access)
      @secret_key = get_secret_key(secret)
      @host = "elasticmapreduce.#@region.amazonaws.com"
      @protocol = {:secure => true}.merge(options)[:secure] ? 'https' : 'http'
    end

    def submit(ruby_params)
      aws_params = AwsSession.convert_ruby_to_aws(ruby_params)
      signed_params = sign_params(aws_params)
      aws_request = AwsRequest.new(self)
      begin
        RestClient.post(aws_request.url, signed_params, aws_request.headers)
      rescue RestClient::BadRequest => e
        raise ArgumentError, AwsSession.parse_error_response(e.http_body)
      end
    end

    def ==(other)
      return false unless other.is_a? AwsSession
      return false unless @access_key == other.access_key
      return false unless @secret_key == other.secret_key
      return false unless @host == other.host
      return false unless @protocol == other.protocol
      true
    end

    private

    def get_access_key(access)
      return access if access
      return ENV['AWS_ACCESS_KEY_ID'] if ENV['AWS_ACCESS_KEY_ID']
      raise MissingKeyError, 'Please provide an access key or set AWS_ACCESS_KEY_ID.'
    end

    def get_secret_key(secret)
      return secret if secret
      return ENV['AWS_SECRET_ACCESS_KEY'] if ENV['AWS_SECRET_ACCESS_KEY']
      raise MissingKeyError, 'Please provide a secret key or set AWS_SECRET_ACCESS_KEY.'
    end

    # (Used from RightScale's right_aws gem.)
    # EC2, SQS, SDB and EMR requests must be signed by this guy.
    # See: http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/index.html?REST_RESTAuth.html
    #      http://developer.amazonwebservices.com/connect/entry.jspa?externalID=1928
    def sign_params(service_hash)
      service_hash.merge!({
        'AWSAccessKeyId' => @access_key,
        'Timestamp' => Time.now.utc.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'SignatureVersion' => '2',
        'SignatureMethod' => 'HmacSHA256'
      })
      canonical_string = service_hash.keys.sort.map do |key|
        "#{AwsSession.aws_escape(key)}=#{AwsSession.aws_escape(service_hash[key])}"
      end.join('&')
      string_to_sign = "POST\n#{@host.downcase}\n/\n#{canonical_string}"
      signature = AwsSession.aws_escape(Base64.encode64(OpenSSL::HMAC.digest("sha256", @secret_key, string_to_sign)).strip)
      "#{canonical_string}&Signature=#{signature}"
    end

    # (Used from RightScale's right_aws gem)
    # Escape a string according to Amazon's rules.
    # See: http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/index.html?REST_RESTAuth.html
    def self.aws_escape(param)
      param.to_s.gsub(/([^a-zA-Z0-9._~-]+)/n) do
        '%' + $1.unpack('H2' * $1.size).join('%').upcase
      end
    end

    # Since we use the same structure as AWS, we can generate AWS param names
    # from the Ruby versions of those names (and the param nesting).
    def self.convert_ruby_to_aws(params)
      result = {}
      params.each do |key, value|
        case value
          when Array
            prefix = "#{camelize(key.to_s)}.member"
            value.each_with_index do |item, index|
              if item.is_a?(String)
                result["#{prefix}.#{index+1}"] = item
              else
                convert_ruby_to_aws(item).each do |nested_key, nested_value|
                  result["#{prefix}.#{index+1}.#{nested_key}"] = nested_value
                end
              end
            end
          when Hash
            prefix = "#{camelize(key.to_s)}"
            convert_ruby_to_aws(value).each do |nested_key, nested_value|
              result["#{prefix}.#{nested_key}"] = nested_value
            end
          else
            result[camelize(key.to_s)] = value
        end
      end
      result
    end

    # (Used from Rails' ActiveSupport)
    def self.camelize(word)
      word.to_s.gsub(/\/(.?)/) { '::' + $1.upcase }.gsub(/(^|_)(.)/) { $2.upcase }
    end

    # AWS error responses all follow the same form.  Extract the message from
    # the error document.
    def self.parse_error_response(error_xml)
      xml_doc = Nokogiri::XML(error_xml)
      xml_doc.remove_namespaces!
      xml_doc.xpath('/ErrorResponse/Error/Message').text
    end

  end

end
