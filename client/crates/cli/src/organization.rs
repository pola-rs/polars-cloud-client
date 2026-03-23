use anyhow::anyhow;
use client_core::{ApiResult, CTRL_PLN_CLIENT_GLOBAL};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{
    OrganizationCreateArgs, OrganizationModel, OrganizationQueryArgs, Pagination,
};
use polars_backend_client::client::ApiClient;

use crate::get_user_input;

pub async fn get_all_organizations(name: Option<String>) -> ApiResult<Vec<OrganizationModel>> {
    CTRL_PLN_CLIENT_GLOBAL
        .call_paginated_async(|client: &ApiClient, page: i64| {
            // TODO: offset is overridden later by (page - 1) * limit, confusing
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let query = OrganizationQueryArgs { name: name.clone() };
            client.get_organizations(pagination, query)
        })
        .await
}

pub async fn get_organization_by_name(name: String) -> ApiResult<Option<OrganizationModel>> {
    Ok(get_all_organizations(Some(name.clone()))
        .await?
        .into_iter()
        .find(|o| o.name == name))
}

pub async fn set_up_organization(
    organization_name: Option<String>,
) -> ApiResult<OrganizationModel> {
    let name = if let Some(name) = organization_name {
        name
    } else {
        get_user_input("Enter organization name: ").await?
    };

    CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client: &ApiClient| {
            client.create_organization(OrganizationCreateArgs { name })
        })
        .await
}

pub async fn print_organizations() -> ApiResult<()> {
    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(vec!["NAME", "ID"]);

    for org in get_all_organizations(None).await? {
        table.add_row(vec![org.name, org.id.to_string()]);
    }

    println!("{table}");

    Ok(())
}

pub async fn print_organization_details(name: String) -> ApiResult<()> {
    let Some(organization) = get_organization_by_name(name.clone()).await? else {
        return Err(anyhow!("No organization with the name {name} was found").into());
    };

    println!("{:#?}", organization);

    Ok(())
}

pub async fn delete_organization(name: String) -> ApiResult<()> {
    let Some(organization) = get_organization_by_name(name.clone()).await? else {
        return Err(anyhow!("No organization with the name {name} was found").into());
    };

    CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client: &ApiClient| client.delete_organization(organization.id))
        .await?;

    Ok(())
}
