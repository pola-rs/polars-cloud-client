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

/// The organization to act in: the named one, or the only one you have when there is no ambiguity.
pub async fn resolve_organization(
    client: &Client,
    name: Option<String>,
) -> ApiResult<OrganizationModel> {
    let Some(name) = name else {
        let mut organizations = get_all_organizations(client, None).await?;

        return match organizations.len() {
            0 => Err(anyhow!(
                "You have no organizations yet. Run `pc organization setup --name <name>` first."
            )
            .into()),
            1 => {
                let organization = organizations.remove(0);
                println!("Using organization {}.", organization.name);
                Ok(organization)
            },
            _ => {
                let names: Vec<_> = organizations.into_iter().map(|o| o.name).collect();
                Err(anyhow!(
                    "You have several organizations. Add `-o <name>` to say which one: {}.",
                    names.join(", ")
                )
                .into())
            },
        };
    };

    match get_organization_by_name(client, name.clone()).await? {
        Some(organization) => Ok(organization),
        None => Err(anyhow!(
            "No organization with the name {name} was found. Run `pc organization setup --name \
             {name}` to create it."
        )
        .into()),
    }
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

    let organization = client
        .create_organization(OrganizationCreateArgs { name })
        .await?;

    println!("Created organization {}.", organization.name);

    Ok(organization)
}

pub async fn print_organizations(client: &Client) -> ApiResult<()> {
    let organizations = get_all_organizations(client, None).await?;

    if organizations.is_empty() {
        println!("No organizations yet. Run `pc organization setup --name <name>` to create one.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(vec!["NAME", "ID"]);

    for org in organizations {
        table.add_row(vec![org.name, org.id.to_string()]);
    }

    println!("{table}");

    Ok(())
}

pub async fn print_organization_details(client: &Client, name: String) -> ApiResult<()> {
    let Some(organization) = get_organization_by_name(client, name.clone()).await? else {
        return Err(anyhow!("No organization with the name {name} was found").into());
    };

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.add_row(vec!["Name", &organization.name]);
    table.add_row(vec!["ID", &organization.id.to_string()]);
    table.add_row(vec!["Description", &organization.description]);
    table.add_row(vec!["Tier", &format!("{:?}", organization.tier)]);
    table.add_row(vec!["Created at", &organization.created_at.to_string()]);

    println!("{table}");

    Ok(())
}

pub async fn delete_organization(client: &Client, name: String) -> ApiResult<()> {
    let Some(organization) = get_organization_by_name(client, name.clone()).await? else {
        return Err(anyhow!("No organization with the name {name} was found").into());
    };

    client.delete_organization(organization.id).await?;

    println!("Deleted organization {}.", organization.name);

    Ok(())
}
