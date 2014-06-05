#require 'boextension.rb'

module Com::Sap::Sbo::Bofrw::Jaw::Bo::Gen

class EmployeesInfo < BOExtension
    def setup
    end 

    def initialize
        p "create class of EmployeesInfo"

    end
    p "loading class " + self.to_s
    
    def after_command
        p "==>after_command"
    end
end
end
