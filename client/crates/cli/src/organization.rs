use anyhow::anyhow;
use client_core::{ApiResult, Client};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{OrganizationCreateArgs, OrganizationModel};

use crate::get_user_input;

pub async fn get_all_organizations(
    client: &Client,
    name: Option<String>,
) -> ApiResult<Vec<OrganizationModel>> {
    client.get_organizations(name).await
}

pub async fn get_organization_by_name(
    client: &Client,
    name: String,
) -> ApiResult<Option<OrganizationModel>> {
    Ok(get_all_organizations(client, Some(name.clone()))
        .await?
        .into_iter()
        .find(|o| o.name == name))
}

pub async fn set_up_organization(
    client: &Client,
    organization_name: Option<String>,
) -> ApiResult<OrganizationModel> {
    let name = if let Some(name) = organization_name {
        name
    } else {
        get_user_input("Enter organization name: ").await?
    };

    client
        .create_organization(OrganizationCreateArgs { name })
        .await
}

pub async fn print_organizations(client: &Client) -> ApiResult<()> {
    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(vec!["NAME", "ID"]);

    for org in get_all_organizations(client, None).await? {
        table.add_row(vec![org.name, org.id.to_string()]);
    }

    println!("{table}");

    Ok(())
}

pub async fn print_organization_details(client: &Client, name: String) -> ApiResult<()> {
    let Some(organization) = get_organization_by_name(client, name.clone()).await? else {
        return Err(anyhow!("No organization with the name {name} was found").into());
    };

    println!("{:#?}", organization);

    Ok(())
}

pub async fn delete_organization(client: &Client, name: String) -> ApiResult<()> {
    let Some(organization) = get_organization_by_name(client, name.clone()).await? else {
        return Err(anyhow!("No organization with the name {name} was found").into());
    };

    client.delete_organization(organization.id).await?;

    Ok(())
}
