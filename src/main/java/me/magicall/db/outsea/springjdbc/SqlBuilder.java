package me.magicall.db.outsea.springjdbc;

import me.magicall.db.outsea.SqlConfig;

import java.util.Map;

@FunctionalInterface
public interface SqlBuilder<C extends SqlConfig> {

	class ParamedSqlAndParams {
		public final String paramedSql;
		public final Map<String, ?> params;

		public ParamedSqlAndParams(final String paramedSql, final Map<String, ?> params) {
			super();
			this.paramedSql = paramedSql;
			this.params = params;
		}
	}

	ParamedSqlAndParams buildSql(C sqlConfig);
}
