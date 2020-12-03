package model.dao.impl;

import com.mysql.jdbc.Statement;
import db.DbException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import model.entities.Seller;
import model.dao.SellerDao;
import db.DB;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import model.entities.Department;

public class SellerDaoJDBC implements SellerDao{
    
    private Connection conn;

    public SellerDaoJDBC(Connection conn){
        this.conn = conn;
    }
    
    @Override
    public void insert(Seller obj) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try{
            st = conn.prepareStatement("INSERT INTO seller "
                    + "(name, email, birthdate, basesalary, departmentid) "
                    + "values(?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
        
            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setInt(5, obj.getDepartment().getId());
            
            int rowsAffected = st.executeUpdate();
            
            if (rowsAffected > 0) {
                rs = st.getGeneratedKeys();
                if (rs.next()){
                    int id = rs.getInt(1);
                    obj.setId(id);
                }
            } else {
                throw new DbException("Unexpected error! No rows were affected!");
            }

        } catch (SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    @Override
    public void update(Seller obj) {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE seller set "
                    + "name = ?, "
                    + "email = ?, "
                    + "birthdate = ?, "
                    + "basesalary = ?, "
                    + "departmentid = ? "
                    + "where id = ?");
            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setInt(5, obj.getDepartment().getId());
            st.setInt(6, obj.getId());
            
            st.executeUpdate();

        } catch (SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public void deleteById(Integer id) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement("DELETE from seller where id = ?");
            st.setInt(1, id);
            st.executeUpdate();

        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public Seller findById(Integer id) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try{
            st = conn.prepareStatement("select s.*, d.name as DepName "
                    + "from seller s, "
                    + "     department d "
                    + "where s.departmentid = d.id "
                    + "and s.id = ?");
            st.setInt(1, id);
            rs = st.executeQuery();
            
            if(rs.next()){
                Department dep = instantiateDepartment(rs);
                Seller obj = instantiateSeller(rs, dep);
                return obj;
            }
            return null;
            
        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    @Override
    public List<Seller> findAll() {
        PreparedStatement st = null;
        ResultSet rs = null;
        try{
            st = conn.prepareStatement("select s.*, d.name as DepName "
                    + "from seller s, "
                    + "     department d "
                    + "where s.departmentid = d.id "
                    + "order by s.name");
            rs = st.executeQuery();
            
            List<Seller> sellers = new ArrayList<>(); 
            Map<Integer, Department> uniqueDepartments = new HashMap<>();
            
            while (rs.next()){
                Department dep = uniqueDepartments.get(rs.getInt("DepartmentId"));
                if (dep == null){
                    dep = instantiateDepartment(rs);
                }
                Seller seller = instantiateSeller(rs, dep);
                sellers.add(seller);
            }
            return sellers;

        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    @Override
    public List<Seller> findByDepartment(Department department) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try{
            st = conn.prepareStatement("select s.*, d.name as DepName "
                    + "from seller s, "
                    + "     department d "
                    + "where s.departmentid = d.id "
                    + "and d.id = ? "
                    + "order by s.name");
            st.setInt(1, department.getId());
            rs = st.executeQuery();
            
            List<Seller> sellers = new ArrayList<>(); 
            Map<Integer, Department> uniqueDepartments = new HashMap<>();
            
            while (rs.next()){
                Department dep = uniqueDepartments.get(rs.getInt("DepartmentId"));
                if (dep == null){
                    dep = instantiateDepartment(rs);
                }
                Seller seller = instantiateSeller(rs, dep);
                sellers.add(seller);
            }
            return sellers;

        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
            Seller obj = new Seller();
            obj.setId(rs.getInt("Id"));
            obj.setName(rs.getString("Name"));
            obj.setEmail(rs.getString("Email"));
            obj.setBaseSalary(rs.getDouble("BaseSalary"));
            obj.setBirthDate(rs.getDate("BirthDate"));
            obj.setDepartment(dep);
            return obj;
    }

    private Department instantiateDepartment(ResultSet rs) throws SQLException {
            Department dep = new Department();
            dep.setId(rs.getInt("DepartmentId"));
            dep.setName(rs.getString("DepName"));
            return dep;
    }

}
