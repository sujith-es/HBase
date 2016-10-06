package Assignment;

public class EmployeeParser {

	private Integer empId;
	private String fullName;
	private Float salary;
	private String laneName;
	private String zipCode;
	private String areaName;

	public void parse(String record) {
		String[] value = record.split(",");
		empId = new Integer(value[0]);
		fullName = value[1];
		salary = new Float(value[2]);
		laneName = value[3];
		zipCode = value[4];
		areaName = value[5];
	}

	public Float getSalary() {
		return salary;
	}

	public void setSalary(Float salary) {
		this.salary = salary;
	}

	public Integer getEmpId() {
		return empId;
	}

	public void setEmpId(Integer empId) {
		this.empId = empId;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getLaneName() {
		return laneName;
	}

	public void setLaneName(String laneName) {
		this.laneName = laneName;
	}

	public String getZipCode() {
		return zipCode;
	}

	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}

}
