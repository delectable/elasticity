module Elasticity

  class S3DistCpStep < CustomJarStep

    def initialize(options)
      @name = 'Elasticity S3DistCp Step'
      @jar = '/home/hadoop/lib/emr-s3distcp-1.0.jar'
      @arguments = []
      options.each do |argument, value|
        @arguments << '--arg' << argument.to_s << '--arg' << value
      end
    end

  end

end